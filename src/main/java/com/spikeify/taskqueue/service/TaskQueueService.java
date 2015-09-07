package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;

/**
 * Adds and retrieves tasks from queue
 */
public interface TaskQueueService {

	/**
	 * Adds task to queue
	 * @param job to be executed
	 * @return added task
	 */
	QueueTask add(Task job, String queueName);

	/**
	 * Gets next task to be executed
	 * @return task to be executed or null if no task found
	 */
	QueueTask next(String queueName);


	/**
	 * Transitions task from current state to new state
	 * This is done in transaction so only one thread can change the state (this is then the worker thread for this task)
	 *
	 * @param task to transition state
	 * @param state to transition to
	 * @return true if transition successed, false if task could not be transitioned
	 */
	boolean transition(QueueTask task, TaskState state);

	/**
	 * Removes task from database
	 * @param task to be removed
	 */
	void remove(QueueTask task);
}
