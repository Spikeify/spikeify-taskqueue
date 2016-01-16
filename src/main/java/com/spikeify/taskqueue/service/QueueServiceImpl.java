package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.QueueSettings;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple wrapper to utilize one or more queues
 */
public class QueueServiceImpl implements QueueService {

	final Logger log = Logger.getLogger(QueueServiceImpl.class.getName());

	private final TaskQueueService queues;

	private final TaskQueueManager manager;

	/**
	 * active queues
	 */
	private final String[] queueNames;

	/**
	 * Register queues with default settings
	 *
	 * @param queueManager queue task manager to be used
	 * @param queueService queue task service to be used
	 * @param queuesToRegister list of queues this instance is working with
	 */
	public QueueServiceImpl(TaskQueueManager queueManager,
	                        TaskQueueService queueService,
	                        String... queuesToRegister) {

		this(queueManager, queueService, null, queuesToRegister);
	}

	/**
	 * Start queues with given settings
	 *
	 * @param queueManager queue task manager to be used
	 * @param queueService queue task service to be used
	 * @param queuesToRegister list of queues this instance is working with
	 */
	public QueueServiceImpl(TaskQueueManager queueManager,
	                        TaskQueueService queueService,
	                        QueueSettings settings,
	                        String... queuesToRegister) {

		queues = queueService;
		manager = queueManager;

		queueNames = queuesToRegister;

		for (String queue : queueNames) {
			manager.register(queue, settings, true); // make sure queue exists ... start it up ... if first time around
		}
	}

	// JOB overview stuff

	@Override
	public QueueTask addJob(Job newJob, String queueName) {

		return queues.add(newJob, queueName);
	}

	@Override
	public List<QueueTask> listJobs(TaskState state, String queueName) {

		return queues.list(state, queueName);
	}

	@Override
	public void settings(QueueSettings settings, String... queues) {

		if (queues == null || queues.length == 0) {
			queues = queueNames;
		}

		for (String queue: queues) {
			manager.set(queue, settings);
			try {
				// queue will be restarted in order to settings take affect
				manager.start(queue);
			}
			catch (InterruptedException e) {
				log.log(Level.SEVERE, "Failed to restart queue when altering settings!", e);
			}
		}
	}

	// QUEUE management stuff

	@Override
	public QueueInfo info(String queueName) {

		return manager.info(queueName);
	}

	@Override
	public QueueInfo resetStatistics(String queueName) {

		manager.resetStatistics(queueName, false);
		return manager.info(queueName);
	}

	@Override
	public void start(String... queues) {

		if (queues == null || queues.length == 0) {
			queues = queueNames;
		}

		for (String queueName : queues) {
			log.info("Starting queue: " + queueName);

			try {
				// if queue should be started than start it up (prevent double start / restart action)
				if (!manager.isRunning(queueName)) {
					manager.start(queueName);
				}
				else {
					log.info("Queue already running ... skipping start!");
				}
			}
			catch (InterruptedException e) {
				log.log(Level.SEVERE, "Failed to start queue!", e);
			}
		}
	}

	@Override
	public void restart(String queueName) {

		try {
			manager.start(queueName); // if not started then simple start ... otherwise restart ...
		}
		catch (InterruptedException e) {
			log.log(Level.SEVERE, "Failed to restart queue!", e);
		}
	}

	@Override
	public void stop(String... queues) {

		if (queues == null || queues.length == 0) {
			queues = queueNames;
		}

		try {

			for (String queue : queues) {
				log.info("Stopping queue: " + queue);
				manager.stop(queue);
			}
		}
		catch (InterruptedException e) {
			log.log(Level.SEVERE, "Failed to stop queue!", e);
		}
	}

	@Override
	public void check() {

		try {

			for (String queueName : queueNames) {
				manager.check(queueName);
			}
		}
		catch (InterruptedException e) {
			log.log(Level.SEVERE, "Failed to check queue!", e);
		}
	}
}
