package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TestHelper;
import com.spikeify.taskqueue.TestTask;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class DefaultTaskQueueServiceTest {

	private static final String QUEUE = "test";
	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();

		spikeify.truncateSet(QueueTask.class);
	}

	@Test
	public void testAdd() throws Exception {

		TaskQueueService service = new DefaultTaskQueueService(spikeify);

		Job dummy = new TestTask(0);

		for (int i = 0; i < 5; i++) {
			// add multiple tasks to queue "test"
			service.add(dummy, QUEUE);
		}

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

		// Simple single threaded get next test ... put and get
		TaskQueueService service = new DefaultTaskQueueService(spikeify);

		for (int i = 0; i < 5; i++) {
			// add multiple tasks to queue "test"
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		for (int i = 0; i < 5; i++) {
			QueueTask task = service.next(QUEUE);
			assertNotNull(task);

			service.transition(task, TaskState.running);
			service.transition(task, TaskState.finished); // get it done ...
		}

		assertNull(service.next(QUEUE)); // no tasks left
	}

	@Test
	public void testNextMultipleWorkers() throws Exception {

		// Simple single threaded get next test ... put and get
		TaskQueueService service = new DefaultTaskQueueService(spikeify, 5); // 5 workers


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
	public void testTransition() throws Exception {

	}

	@Test
	public void testRemove() throws Exception {

	}
}