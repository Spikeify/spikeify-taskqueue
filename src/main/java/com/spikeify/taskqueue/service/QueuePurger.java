package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.entities.TaskState;

import java.util.logging.Logger;

public class QueuePurger implements Runnable {

	private static final Logger log = Logger.getLogger(QueuePurger.class.getSimpleName());

	private final TaskQueueService queues;
	private String queueName;
	private final TaskState state;
	private final int age;

	public QueuePurger(TaskQueueService queueService, String queue, TaskState taskState, int taskAge) {

		queues = queueService;

		queueName = queue;
		state = taskState;
		age = taskAge;
	}

	@Override
	public void run() {

		int count = queues.purge(state, age, queueName);
		if (count > 0) {
			log.info("[" + queueName + "] purge: " + count + " " + state + " task(s).");
		}
	}
}
