package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.entities.QueueInfo;

import java.util.List;

public interface TaskQueueManager {

	/**
	 * Registers queue to be monitored
	 * @param queueName name of queue
	 * @return registered queue info
	 */
	QueueInfo register(String queueName);

	/**
	 * Lists queues registered
	 * @param active - true list active queues, false - list disabled queues
	 * @return list of registered queues
	 */
	List<QueueInfo> list(boolean active);

	/**
	 * Removes queue from monitoring
	 * @param queueName to be removed
	 */
	void unregister(String queueName);

	/**
	 * Enables queue to be run
	 * @param queueName name of queue
	 */
	void start(String queueName);

	/**
	 * Disables queue - stops
	 * @param queueName to be stopped
	 */
	void stop(String queueName);

	/**
	 * Must be run in regular bases ...
	 * should purge finished/failed tasks and restartart hanged tasks
	 */
	void run();
}
