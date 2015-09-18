package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.*;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.entities.TaskStatistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DefaultTaskQueueServiceTest {

	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();
		spikeify.truncateNamespace("test");
	}

	@After
	public void tearDown() {

		spikeify.truncateNamespace("test");
	}

	@Test
	public void testAdd() throws Exception {

		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		DefaultTaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);

		String QUEUE = "testAdd";
		manager.register(QUEUE);

		Job dummy = new TestTask(0);

		for (int i = 0; i < 5; i++) {
			// add multiple tasks to queue "test"
			service.add(dummy, QUEUE);
		}

		// check queue info
		QueueInfo info = manager.info(QUEUE);

		assertEquals(5, info.getQueuedTasks());
		assertEquals(0, info.getRunningTasks());

		// list all tasks
		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		assertEquals(5, list.size());

		Set<String> ids = new HashSet<>();
		for (QueueTask task: list) {

			Job job = task.getJob();
			assertTrue(job instanceof TestTask);
			assertEquals(((TestTask) job).getProperty(), 0);

			assertEquals(QUEUE, task.getQueue());

			ids.add(task.getId());
		}

		assertEquals(5, ids.size());
	}

	@Test
	public void testNext() throws Exception {

		String QUEUE = "testNext";
		// Simple single threaded get next test ... put and get
		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		DefaultTaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE);

		for (int i = 0; i < 5; i++) {
			// add multiple tasks to queue "test"
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		QueueInfo info = manager.info(QUEUE);
		assertEquals(5, info.getQueuedTasks());

		for (int i = 0; i < 5; i++) {
			QueueTask task = service.next(QUEUE);
			assertNotNull(task);

			service.transition(task, TaskState.running);

			info = manager.info(QUEUE);
			assertEquals(4 - i, info.getQueuedTasks());
			assertEquals(1, info.getRunningTasks());

			service.transition(task, TaskState.finished); // get it done ...

			info = manager.info(QUEUE);
			assertEquals(4 - i, info.getQueuedTasks());
			assertEquals(0, info.getRunningTasks());
			assertEquals(1 + i, info.getFinishedTasks());
		}

		assertNull(service.next(QUEUE)); // no tasks left
	}

	@Test
	public void testNextMultipleWorkers() throws Exception {

		String QUEUE = "testNextMultipleWorkers";
		// Simple single threaded get next test ... put and get
		TaskQueueService service = new DefaultTaskQueueService(spikeify); // 5 workers


		for (int i = 0; i < 5; i++) {
			// add multiple tasks to queue "test"
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		boolean found = false;
		for (int i = 0; i < 5; i++) {
			QueueTask task = service.next(QUEUE);
			assertNotNull(task);

			// at least once ... the job number should not be the same as the index
			TestTask job = (TestTask) task.getJob();
			if (job.getProperty() != i) {
				found = true;
			}

			service.transition(task, TaskState.running);
			service.transition(task, TaskState.finished); // get it done ...
		}

		assertTrue("No random job selection ...", found);

		assertNull(service.next(QUEUE)); // no tasks left
	}

	@Test
	public void purgeFinishedTasksTest() {

		String QUEUE = "purgeFinishedTasksTest";
		AtomicInteger failed = new AtomicInteger(0);
		AtomicInteger completed = new AtomicInteger(0);

		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		TaskExecutorService executor = new DefaultTaskExecutorService(service, QUEUE);
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE);

		// add tasks to queue
		int NUMBER_OF_JOBS = 10;
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		execute(executor, completed, failed);

		// check ... 10 finished tasks should be present
		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		assertEquals(10, list.size());

		Set<String> ids = new HashSet<>();
		for (QueueTask task : list) {
			assertEquals(QUEUE, task.getQueue());
			assertEquals(TaskState.finished, task.getState());
			assertEquals(1, task.getRunCount());

			ids.add(task.getId());
		}

		assertEquals(10, ids.size());

		QueueInfo info = manager.info(QUEUE);
		assertEquals(10, info.getFinishedTasks());


		TaskStatistics purged = service.purge(TaskState.finished, 0, QUEUE);
		assertNotNull(purged);
		assertEquals(10, purged.getCount());

		// check info
		info = manager.info(QUEUE);
		assertEquals(0, info.getFinishedTasks());
		assertEquals(10, info.getTotalFinished());

		TaskStatistics stats = info.getStatistics(TaskState.finished);
		assertEquals(10, stats.getCount());
		assertTrue(stats.getMinJobRunTime() <= stats.getMaxJobRunTime());
		assertTrue(stats.getMinExecutionTime() <= stats.getMaxExecutionTime());
		assertTrue(stats.getMaxExecutionTime() <= stats.getTotalExecutionTime());

		list = spikeify.scanAll(QueueTask.class).now();
		assertEquals(0, list.size());
	}

	private int execute(TaskExecutorService service, AtomicInteger completed, AtomicInteger failed) {

		int completedJobs = 0;

		TaskResult result;
		do {

			//Thread.sleep(100); // wait 100ms ..
			result = service.execute(new TestTaskContext());

			// count finished jobs by this worker only
			if (result != null)
			{
				switch (result.getState()) {
					case ok:
						completed.incrementAndGet();
						completedJobs ++;
						break;

					case failed:
						failed.incrementAndGet();
						break;
				}
			}
		}
		while (result != null);
		return completedJobs;
	}
}