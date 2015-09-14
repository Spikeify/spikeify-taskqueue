package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.utils.Assert;

import java.util.logging.Logger;

public class QueueScheduler implements Runnable {

	private static final Logger log = Logger.getLogger(QueueScheduler.class.getSimpleName());

	private final TaskQueueService queues;

	private final TaskExecutorService executor;

	public QueueScheduler(TaskQueueService queueService, String queueName) {

		Assert.notNull(queueService, "Missing queue service!");

		queues = queueService;
		executor = new DefaultTaskExecutorService(queues, queueName);
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

		int count = 0;
		long startTime = System.currentTimeMillis();

		TaskResult result;

		do {
			// while there are tasks to be executed ... continue with execution
			result = executor.execute(context);

			if (result != null) {
				count++; // count tasks executed
			}
		}
		while (result != null);

		log.info("No tasks found ... stopping after: " + count + ", execution(s).");
	}

	// TODO:
	private TaskContext getContext() {

		return null;
	}
}
