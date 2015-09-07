package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.*;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskResultState;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultithreadTaskExecutorServiceTest {

	Logger log = Logger.getLogger(MultithreadTaskExecutorServiceTest.class.getSimpleName());

	private static final int NUMBER_OF_JOBS = 1000;
	private static final String QUEUE = "test";
	private TaskQueueService service;
	private TaskExecutorService executor;

	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();
		service = new DefaultTaskQueueService(spikeify);
		executor = new DefaultTaskExecutorService(service, QUEUE);

		spikeify.truncateSet(QueueTask.class);
	}

	@Test
	public void runSingleExecutor() {

		AtomicInteger count = new AtomicInteger(0);
		AtomicBoolean finished = new AtomicBoolean(false);

		// add tasks to queue
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Task dummy = new TestTask("" + i);
			service.add(dummy, QUEUE);
		}

		DefaultTaskExecutorService worker = new DefaultTaskExecutorService(service, QUEUE);
		execute(worker, 1, count, finished);

		await().untilTrue(finished);
	}

	@Test
	public void runMultipleExecutors() throws Exception {

		AtomicInteger count = new AtomicInteger(0);
		AtomicBoolean finished = new AtomicBoolean(false);

		// add tasks to queue
		for (int i = 0; i < NUMBER_OF_JOBS; i++) {
			Task dummy = new TestTask("" + i);
			service.add(dummy, QUEUE);
		}

		// create 3 workers
		int WORKERS = 3;
		Thread[] threads = new Thread[WORKERS];
		int[] workCompleted = new int[WORKERS];

		for (int i = 0; i < WORKERS; i++) {
			final int index = i;

			Runnable runnable = new Runnable() {
				@Override
				public void run() {

					TaskExecutorService worker = new DefaultTaskExecutorService(service, QUEUE);
					workCompleted[index] = execute(worker, index, count, finished);
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

		await().untilTrue(finished);

		int total = 0;
		for (int i = 0; i < WORKERS; i++) {

			assertTrue("To much work for worker: " + i + "=" + workCompleted[i] + " job(s) completed!", workCompleted[i] > 300); //  each worker should complete aprox. 1/3 of the workload
			total = total + workCompleted[i];
		}
		// number of completed jobs should be same as number given jobs
		assertEquals(NUMBER_OF_JOBS, total);
	}

	private int execute(TaskExecutorService service, int index, AtomicInteger count, AtomicBoolean finished) {

		AtomicInteger completedJobs = new AtomicInteger(0);

		log.info("Worker: " + index + ", STARTED!");
		// repeat thread until finished ...
		while (!finished.get()) {

			//Thread.sleep(100); // wait 100ms ..
			TaskContext context = new TaskThreadContext(Thread.currentThread());
			TaskResult result = service.execute(null);

			// count finished jobs by this worker only
			if (result != null &&
				TaskResultState.ok.equals(result.getState())) {
				count.incrementAndGet();
				completedJobs.incrementAndGet();
			}

			// exit once the final target is hit
			finished.set(count.get() == NUMBER_OF_JOBS);
		}

		log.info("Worker: " + index + ", FINISHED: " + completedJobs.get() + " job(s)");
		return completedJobs.get();
	}
}
