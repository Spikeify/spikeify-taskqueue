package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.utils.Assert;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueueScheduler implements Runnable {

	private static final Logger log = Logger.getLogger(QueueScheduler.class.getSimpleName());

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

		TaskContext context = getContext();

		log.fine("Starting task execution ...");

		int successCount = 0;
		int allCount = 0;
		boolean interrupted = false;

		TaskResult result;

		do {
			// while there are tasks to be executed ... continue with execution

			ExecutorService service = Executors.newSingleThreadExecutor();
			WorkerThread worker = new WorkerThread(context);
			service.execute(worker);

			try {
				// little trick ... trigger shutdown to await time out
				// if time out occurs try gracefully terminating task ...
				// if not return failed result ...
				// task will stay in running state ... and Purger should take care of it (to put it into failed state)
				service.shutdown();
				if (!service.awaitTermination(taskTimeout, TimeUnit.SECONDS)) {

					// send interrupt signal
					context.interrupt();

					// prolong for some time before giving up
					if (!service.awaitTermination(taskInterruptTimeout, TimeUnit.SECONDS)) {

						// task is stuck ... kill it
						service.shutdownNow();
						result = worker.getResult();

						if (result == null) {
							result = TaskResult.failed();
							worker.reset();
						}
					}
					else {
						// task finished successfully ... get the result
						result = worker.getResult();
					}
				}
				else {
					// task finished successfully ... get the result
					result = worker.getResult();
				}
			}
			catch (InterruptedException e) {
				log.log(Level.SEVERE, "Task has been timed out ...");
				result = TaskResult.failed();
			}

			if (result != null) {

				if (TaskResultState.interrupted.equals(result.getState())) {
					interrupted = true;
					break;
				}

				if (TaskResultState.ok.equals(result.getState())) {
					successCount++;
				}

				allCount++; // count tasks executed
			}
		}
		while (result != null);

		if (interrupted) {
			log.fine("Interrupted after: " + successCount + "/" + allCount + " execution(s).");
		}
		else {
			log.fine("No new tasks found, stopping after: " + successCount + "/" + allCount + " execution(s).");
		}
	}

	private TaskContext getContext() {

		return context;
	}

	private class WorkerThread implements Runnable {

		private final TaskContext context;

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

		public void reset() {

			executor.reset();
		}
	}
}
