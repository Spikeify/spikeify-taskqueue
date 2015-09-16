package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.utils.Assert;

import java.util.logging.Logger;

public class QueueScheduler implements Runnable {

	private static final Logger log = Logger.getLogger(QueueScheduler.class.getSimpleName());

	private final TaskQueueService queues;
	private final TaskExecutorService executor;
	private final TaskContext context;

	public QueueScheduler(TaskQueueService queueService, String queueName, TaskContext threadContext) {

		Assert.notNull(queueService, "Missing queue service!");

		queues = queueService;
		executor = new DefaultTaskExecutorService(queues, queueName);
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

		log.info("Starting task execution ...");

		int successCount = 0;
		int allCount = 0;
		boolean interrupted = false;

		TaskResult result;

		do {
			// while there are tasks to be executed ... continue with execution
			result = executor.execute(context);

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
			log.info("Interrupted after: " + successCount + "/" + allCount + " execution(s).");
		}
		else {
			log.info("No tasks found, stopping after: " + successCount + "/" + allCount + " execution(s).");
		}
	}

	private TaskContext getContext() {

		return context;
	}
}
