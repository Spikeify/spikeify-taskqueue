package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.ExecutionContext;
import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueueScheduler implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(QueueScheduler.class);

	private final TaskExecutorService executor;

	private final TaskContext context;

	private final int taskTimeout;

	private final int taskInterruptTimeout;

	public QueueScheduler(TaskExecutorService executorService,
	                      int timeoutInSeconds,
	                      int interruptTimeoutSeconds,
	                      TaskContext threadContext) {

		Assert.notNull(executorService, "Missing queue executor service!");

		executor = executorService;
		taskTimeout = timeoutInSeconds;
		taskInterruptTimeout = interruptTimeoutSeconds;
		context = threadContext;
	}

	/**
	 * Simple loop to execute tasks ...
	 * if no task is available sleep for some time
	 * if next task is available then execute the next
	 */
	@Override
	public void run() {

		log.debug("Starting task execution ...");

		int successCount = 0;
		int allCount = 0;

		TaskResult result;

		do {
			// while there are tasks to be executed ... continue with execution

			if (context.interrupted()) {
				log.warn("Execution was interrupted from outside!");
				break;
			}

			ExecutorService service = Executors.newSingleThreadExecutor();
			TaskContext workerContext = new ExecutionContext(context);

			WorkerThread worker = new WorkerThread(workerContext); // create execution thread
			Future<?> future = service.submit(worker);

			try {
				// little trick ... trigger shutdown to await time out
				// if time out occurs try gracefully terminating task ...
				// if not return failed result ...
				// task will stay in running state ... and Purger should take care of it (to put it into failed state)
				service.shutdown();
				if (!service.awaitTermination(taskTimeout, TimeUnit.SECONDS)) {

					// send interrupt signal
					workerContext.interrupt();

					service.shutdown();
					// prolong for some time before giving up
					if (!service.awaitTermination(taskInterruptTimeout, TimeUnit.SECONDS)) {

						// task is stuck ... kill it
						log.warn("Failed to gracefully interrupt task, killing task instead!");

						future.cancel(true);
						log.info("Task thread killed: canceled=" + future.isCancelled() + ", done=" + future.isDone());
						service.shutdownNow();

						// try to get some result ... if possible ...
						result = worker.getResult();

						if (result == null) {
							result = TaskResult.failed();
						}
					}
					else {
						// task finished successfully ... get the result
						result = worker.getResult();
						future.cancel(true);
						service.shutdownNow();
					}
				}
				else {
					// task finished successfully ... get the result
					result = worker.getResult();
					future.cancel(true);
					service.shutdownNow();
				}
			}
			catch (InterruptedException e) {
				log.error("Task has been timed out ...");
				result = TaskResult.failed();
				future.cancel(true);
				service.shutdownNow();
			}

			if (result != null) {

				if (TaskResultState.ok.equals(result.getState())) {
					successCount++;
				}

				allCount++; // count tasks executed
			}

			if (result == null) {
				log.debug("No new tasks found ... exiting");
			}
			else {
				log.debug("Last task result: " + result);
			}
		}
		while (result != null);

		log.debug("No new tasks found, stopping after: " + successCount + "/" + allCount + " execution(s).");
	}

	private class WorkerThread implements Runnable {

		private TaskContext context;

		private TaskResult result;

		public WorkerThread(TaskContext context) {

			this.context = context;
		}

		public TaskResult getResult() {

			return result;
		}

		@Override
		public void run() {

			result = executor.execute(context);
		}
	}
}
