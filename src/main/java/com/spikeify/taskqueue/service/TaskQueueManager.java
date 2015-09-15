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
	 * Starts queue ...
	 * should be called only once per thread,
	 * if called multiple times then threads are terminated and restated (acts as restart)
	 *
	 * @param queueName name of queue
	 */
	void start(String queueName) throws InterruptedException;

	/**
	 * Disables queue - stops all running tasks/threads
	 * @param queueName to be stopped
	 */
	void stop(String queueName) throws InterruptedException;
}
