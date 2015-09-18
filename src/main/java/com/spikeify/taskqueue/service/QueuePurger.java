package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.entities.QueueSettings;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.entities.TaskStatistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class QueuePurger implements Runnable {

	private static final Logger log = Logger.getLogger(QueuePurger.class.getSimpleName());
	private static final int ADDITIONAL_SLACK = 10; // add 10 seconds ... so purge and time-out in Scheduler can't clash

	private final TaskQueueService queues;
	private final int timeout;
	private String queueName;

	private Map<TaskState, Integer> states = new HashMap<>();

	public QueuePurger(TaskQueueService queueService, String queue, QueueSettings settings) {

		queues = queueService;

		queueName = queue;

		states.put(TaskState.finished, settings.getPurgeSuccessfulAfterMinutes());
		states.put(TaskState.failed, settings.getPurgeFailedAfterMinutes());

		timeout = settings.getTaskTimeoutSeconds();
	}

	@Override
	public void run() {

		for (TaskState state: states.keySet()) {

			int maxAge = states.get(state);
			TaskStatistics purge = queues.purge(state, maxAge, queueName);

			if (purge != null) {

				log.info("[" + queueName + "] purge: " + purge.getCount() + " " + state + " task(s).");
			}
		}

		// check for timed out tasks ...
		List<QueueTask> running = queues.list(TaskState.running, queueName);
		for (QueueTask task : running) {
			if (task.isOlderThanSeconds(timeout + ADDITIONAL_SLACK)) {
				log.info("Found hanged/timed out task: " + task + ", putting into failed state!");
				queues.transition(task, TaskState.failed); // move task to failed state ... so it can be restarted
			}
		}
	}
}
