package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.*;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.entities.TaskState;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class MultithreadTaskExecutorServiceTest {

	Logger log = Logger.getLogger(MultithreadTaskExecutorServiceTest.class.getSimpleName());

	private static final String QUEUE = "test";

	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();
		spikeify.truncateSet(QueueTask.class);
	}

	@Test
	public void runSingleExecutor() {

		AtomicInteger failed = new AtomicInteger(0);
		AtomicBoolean finished = new AtomicBoolean(false);

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);
		DefaultTaskExecutorService executor = new DefaultTaskExecutorService(service, QUEUE);

		int NUMBER_OF_JOBS = 100;

		// add tasks to queue
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		int count = execute(executor, 1, failed);
		assertEquals(NUMBER_OF_JOBS, count);
	}

	@Test
	public void runMultipleExecutors() throws Exception {

		AtomicInteger count = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);

		int NUMBER_OF_JOBS = 100;
		int WORKERS = 3;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify, WORKERS);

		// add tasks to queue
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		// create 3 workers
		Thread[] threads = new Thread[WORKERS];
		int[] workCompleted = new int[WORKERS];

		for (int i = 0; i < WORKERS; i++) {
			final int index = i;

			Runnable runnable = new Runnable() {
				@Override
				public void run() {

					TaskExecutorService worker = new DefaultTaskExecutorService(service, QUEUE);
					workCompleted[index] = execute(worker, index, failed);
				}
			};

			threads[i] = new Thread(runnable);
		}

		for (int i = 0; i < WORKERS; i++) {
			threads[i].start();
		}
		for (int i = 0; i < WORKERS; i++) {
			threads[i].join();
		}

		int total = 0;
		for (int i = 0; i < WORKERS; i++) {

			log.info("Jobs failed: " + failed.get());
		//	assertTrue("To little work for worker [" + i + "], completed: " + workCompleted[i] + " job(s)", workCompleted[i] > 300); //  each worker should complete aprox. 1/3 of the workload
			total = total + workCompleted[i];

		}
		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS, total);
	}

	@Test
	public void runMoreWorkersThanJobs() throws Exception {

		AtomicInteger failed = new AtomicInteger(0);

		int NUMBER_OF_JOBS = 10;
		int WORKERS = 20;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify, WORKERS);

		// add tasks to queue
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		// create 3 workers
		Thread[] threads = new Thread[WORKERS];
		int[] workCompleted = new int[WORKERS];

		for (int i = 0; i < WORKERS; i++) {
			final int index = i;

			Runnable runnable = new Runnable() {
				@Override
				public void run() {

					TaskExecutorService worker = new DefaultTaskExecutorService(service, QUEUE);
					workCompleted[index] = execute(worker, index, failed);
				}
			};

			threads[i] = new Thread(runnable);
		}

		for (int i = 0; i < WORKERS; i++) {
			threads[i].start();
		}
		for (int i = 0; i < WORKERS; i++) {
			threads[i].join();
		}

		log.info("Jobs failed: " + failed.get());

		int total = 0;
		for (int i = 0; i < WORKERS; i++) {

			// assertTrue("To much work for worker: " + i + ", completed" + workCompleted[i] + " job(s) completed!", workCompleted[i] > 300); //  each worker should complete aprox. 1/3 of the workload
			total = total + workCompleted[i];
		}
		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS, total);
	}

	/**
	 * Run FailingTask ... which will fail on every 20 execution ...
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void retryFailedTasksTest() throws InterruptedException {

		AtomicInteger count = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);
		// add tasks to queue
		int NUMBER_OF_JOBS = 200;
		int WORKERS = 5;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify, WORKERS);

		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Job dummy = new FailingTask(i); // job which will fail in case job number is dividable by 20
			service.add(dummy, QUEUE);
		}

		// create 3 workers
		Thread[] threads = new Thread[WORKERS];
		int[] workCompleted = new int[WORKERS];

		for (int i = 0; i < WORKERS; i++) {
			final int index = i;

			Runnable runnable = new Runnable() {
				@Override
				public void run() {

					TaskExecutorService worker = new DefaultTaskExecutorService(service, QUEUE);
					workCompleted[index] = execute(worker, index, failed);
				}
			};

			threads[i] = new Thread(runnable);
		}

		for (int i = 0; i < WORKERS; i++) {
			threads[i].start();
		}
		for (int i = 0; i < WORKERS; i++) {
			threads[i].join();
		}

		int total = 0;
		for (int i = 0; i < WORKERS; i++) {

		//	assertTrue("To little work for worker [" + i + "] has " + workCompleted[i] + " job(s) completed!", workCompleted[i] > 20); //  each worker should complete aprox. 1/3 of the workload
			total = total + workCompleted[i];
		}

		// checked failed tasks
		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		int runCount = 0;
		for (QueueTask task : list) {
			if (task.getRunCount() > 1) {
				runCount = runCount + (task.getRunCount() - 1);
			}
		}

		//assertTrue("Failed and run count are not equal, failed: " + failed.get() + ", reruns: " + runCount, failed.get() == runCount);

		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS, total);
	}

	@Test
	public void purgeFinishedTasksTest() {

		AtomicInteger failed = new AtomicInteger(0);

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);
		DefaultTaskExecutorService executor = new DefaultTaskExecutorService(service, QUEUE);

		// add tasks to queue
		for (int i = 0; i < 10; i++) {
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		execute(executor, 1, failed);

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

		int purged = executor.purge(TaskState.finished);
		assertEquals(10, purged);

		list = spikeify.scanAll(QueueTask.class).now();
		assertEquals(0, list.size());
	}

	private int execute(TaskExecutorService service, int index, AtomicInteger failed) {

		AtomicInteger completedJobs = new AtomicInteger(0);

		log.info("Worker [" + index + "], STARTED!");
		// repeat thread until finished ...
		TaskResult result;
		do {

			//Thread.sleep(100); // wait 100ms ..
			result = service.execute(new TestTaskContext());

			// count finished jobs by this worker only
			if (result != null &&
				TaskResultState.ok.equals(result.getState())) {
				completedJobs.incrementAndGet();
			}
			else if (result != null) {
				// log.info("Worker: " + index + ", job FAILED ... retrying!");
				failed.incrementAndGet();
			}
		}
		while (result != null);

		log.info("Worker [" + index + "], FINISHED: " + completedJobs.get() + " job(s)");
		return completedJobs.get();
	}
}
