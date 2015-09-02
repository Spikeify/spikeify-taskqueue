package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.entities.QueueTask;

/**
 * Adds and retrieves tasks from queue
 */
public interface TaskQueueService {

	/**
	 * Adds task to queue
	 * @param job to be executed
	 * @return added task
	 */
	QueueTask add(Task job);

	/**
	 * Gets next task to be executed
	 * @return task to be executed or null if no task found
	 */
	QueueTask next();

	/**
	 * Removes task from database
	 * @param task to be removed
	 */
	void remove(QueueTask task);
}
