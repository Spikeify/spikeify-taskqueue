package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.entities.TaskStatistics;

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

		TaskStatistics purge = queues.purge(state, age, queueName);
		if (purge != null) {

			log.info("[" + queueName + "] purge: " + purge.getCount() + " " + state + " task(s).");

			// join this data with statistics from given queue
		}
	}
}
