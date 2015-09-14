package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.*;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultithreadTaskExecutorServiceTest {

	Logger log = Logger.getLogger(MultithreadTaskExecutorServiceTest.class.getSimpleName());

	private static final String QUEUE = "test";

	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();
		spikeify.truncateNamespace("test");
	}

	@Test
	public void runSingleExecutor() {

		AtomicInteger completed = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);
		DefaultTaskExecutorService executor = new DefaultTaskExecutorService(service, QUEUE);

		int NUMBER_OF_JOBS = 100;

		// add tasks to queue
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Job dummy = new TestTask(i);
			service.add(dummy, QUEUE);
		}

		int count = execute(executor, completed, failed);
		assertEquals(NUMBER_OF_JOBS, count);
		assertEquals(NUMBER_OF_JOBS, completed.get());
		assertEquals(0, failed.get());
	}

	@Test
	public void runMultipleExecutors() throws Exception {

		AtomicInteger completed = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);

		int NUMBER_OF_JOBS = 500;
		int WORKERS = 3;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);

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
					workCompleted[index] = execute(worker, completed, failed);
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

		//await().untilTrue(new AtomicBoolean(completed.get() >= NUMBER_OF_JOBS));

		log.info("Jobs failed: " + failed.get());

		int total = 0;
		for (int i = 0; i < WORKERS; i++) {

			total = total + workCompleted[i];
			log.info("Worker [" + i + "], FINISHED: " + workCompleted[i] + " job(s)");
		}

		// check if work was distributed evenly
		for (int i = 0; i < WORKERS; i++) {
			assertTrue("To little work for worker [" + i + "], completed: " + workCompleted[i] + " job(s)", workCompleted[i] > 150); //  each worker should complete aprox. 1/3 of the workload
		}

		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS, total);

		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		for (QueueTask task : list) {
			assertEquals("Task was run more than once: " + task.getId(), 1, task.getRunCount());
		}
	}

	@Test
	public void runMoreWorkersThanJobs() throws Exception {

		AtomicInteger completed = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);

		int NUMBER_OF_JOBS = 10;
		int WORKERS = 20;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);

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
					workCompleted[index] = execute(worker, completed, failed);
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

		//await().untilTrue(new AtomicBoolean(completed.get() >= NUMBER_OF_JOBS));

		log.info("Jobs failed: " + failed.get());

		int total = 0;
		for (int i = 0; i < WORKERS; i++) {

			total = total + workCompleted[i];
			log.info("Worker [" + i + "], FINISHED: " + workCompleted[i] + " job(s)");
		}
		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS, total);

		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		for (QueueTask task : list) {
			assertEquals("Task was run more than once: " + task.getId(), 1, task.getRunCount());
		}
	}

	/**
	 * Run FailingTask ... which will fail on every 20 execution ...
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void retryFailedTasksTest() throws InterruptedException {

		AtomicInteger completed = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);
		// add tasks to queue
		int NUMBER_OF_JOBS = 200;
		int WORKERS = 5;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);

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
					workCompleted[index] = execute(worker, completed, failed);
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

			total = total + workCompleted[i];
			log.info("Worker [" + i + "], FINISHED: " + workCompleted[i] + " job(s)");
		}

		// check if work was distributed evenly
		for (int i = 0; i < WORKERS; i++) {
			assertTrue("To little work for worker [" + i + "], completed: " + workCompleted[i] + " job(s)", workCompleted[i] > 20); //  each worker should complete aprox. 1/3 of the workload
		}

		// checked failed tasks
		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		int runCount = 0;
		for (QueueTask task : list) {
			if (task.getRunCount() > 1) {
				if (task.getState() == TaskState.finished) {
					runCount = runCount + (task.getRunCount() - 1);
				}
				else {
					runCount = runCount + (task.getRunCount());
				}
			}
		}

		assertTrue("Failed and rerun count are not equal, failed: " + failed.get() + ", reruns: " + runCount, failed.get() == runCount);

		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS, total);
	}

	/**
	 * Run FailingTask ... which will fail on every 20 execution ...
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void retryOrAbandonFailedTasksTest() throws InterruptedException {

		AtomicInteger completed = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);
		// add tasks to queue
		int NUMBER_OF_JOBS = 200;
		int WORKERS = 5;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);

		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Job dummy = new FailingTask(i); // job which will fail in case job number is dividable by 20
			service.add(dummy, QUEUE);

			Job willFail = new FailOnlyTask();
			service.add(willFail, QUEUE);

			Job okTask = new TestTask(i);
			service.add(okTask, QUEUE);
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
					workCompleted[index] = execute(worker, completed, failed);
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

			total = total + workCompleted[i];
			log.info("Worker [" + i + "], FINISHED: " + workCompleted[i] + " job(s)");
		}

		// check if work was distributed evenly
		for (int i = 0; i < WORKERS; i++) {
			assertTrue("To little work for worker [" + i + "], completed: " + workCompleted[i] + " job(s)", workCompleted[i] > 60); //  each worker should complete aprox. 1/3 of the workload
		}

		// checked failed tasks
		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		int runCount = 0;
		for (QueueTask task : list) {
			if (task.getRunCount() > 1) {
				if (task.getState() == TaskState.finished) {
					runCount = runCount + (task.getRunCount() - 1);
				}
				else {
					runCount = runCount + (task.getRunCount());
				}
			}
		}


		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS * 2, total); // 1/3 will always fail

		assertTrue("Failed and rerun count are not equal, failed: " + failed.get() + ", reruns: " + runCount, failed.get() == runCount);
	}

	/**
	 * Run FailingTask ... which will fail on every 20 execution ...
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void retryFailingTasks() throws InterruptedException {

		AtomicInteger completed = new AtomicInteger(0);
		AtomicInteger failed = new AtomicInteger(0);
		// add tasks to queue
		int NUMBER_OF_JOBS = 200;
		int WORKERS = 5;

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);

		// will always fail ...
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {

			Job willFail = new FailOnlyTask();
			service.add(willFail, QUEUE);
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
					workCompleted[index] = execute(worker, completed, failed);
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

			total = total + workCompleted[i];
			log.info("Worker [" + i + "], FINISHED: " + workCompleted[i] + " job(s)");
		}

		// check if work was distributed evenly
		for (int i = 0; i < WORKERS; i++) {
			assertTrue("Worker should not completed any jobs: " + workCompleted[i] + " job(s)", workCompleted[i] == 0); //  each worker should complete aprox. 1/3 of the workload
		}

		// checked failed tasks
		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		int runCount = 0;
		for (QueueTask task : list) {
			if (task.getRunCount() > 1) {
				if (task.getState() == TaskState.finished) {
					runCount = runCount + (task.getRunCount() - 1);
				}
				else {
					runCount = runCount + (task.getRunCount());
				}
			}
		}


		// number of completed jobs should be same as number given jobs
		assertEquals(0, total); // all failed
		assertEquals("Invalid number of task retries", runCount, NUMBER_OF_JOBS * 3); // all task should be retried 3 times before abandon
	}

	@Test
	public void interruptLongRunningTask() throws InterruptedException {

		DefaultTaskQueueService service = new DefaultTaskQueueService(spikeify);
		DefaultTaskExecutorService executor = new DefaultTaskExecutorService(service, QUEUE);

		service.add(new LongRunningTask(), QUEUE);

		Runnable runnable = new Runnable() {
			@Override
			public void run() {

				executor.execute(new TestTaskContext());
			}
		};

		Thread thread = new Thread(runnable);
		thread.start();

		Thread.sleep(1000);

		assertTrue(executor.isRunning());

		executor.interrupt();

		assertFalse(executor.isRunning());
	}

	private int execute(TaskExecutorService service, AtomicInteger completed, AtomicInteger failed) {

		int completedJobs = 0;

		TaskResult result;
		do {

			//Thread.sleep(100); // wait 100ms ..
			result = service.execute(new TestTaskContext());

			// count finished jobs by this worker only
			if (result != null) {
				switch (result.getState()) {
					case ok:
						completed.incrementAndGet();
						completedJobs++;
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
