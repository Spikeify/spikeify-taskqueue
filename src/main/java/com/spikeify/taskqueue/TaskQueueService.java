package com.spikeify.taskqueue;

import com.spikeify.taskqueue.entities.QueueTask;

/**
 * Adds, executes, retries ... queued tasks
 */
public interface TaskQueueService {

	/**
	 * Adds task to queue
	 * @param job to be executed
	 * @return added task
	 */
	QueueTask add(Task job);

	/**
	 * Adds task to queue with certain priority
	 * @param job to be executed
	 * @param priority defines if task is executed prior or later according to other tasks in the queue
	 * @return added task
	 */
	QueueTask add(Task job, TaskPriority priority);

	/**
	 * Gets next task to be executed
	 * @return task to be executed or null if no task found
	 */
	QueueTask next();

	/**
	 * Executes task
	 * @param context environment context
	 * @return task result or null if not applicable
	 */
	TaskResult execute(TaskContext context);
}
