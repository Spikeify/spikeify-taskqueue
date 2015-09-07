package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.TaskState;

/**
 * Takes care of task execution, utilizes TaskQueueService to get next task
 *
 * 1. retrieves next task from queue and locks it for other executors
 * 2. executes task - takes care of failover (task taking to long and other exceptions)
 * 3. unlocks task when execution finishes
 *
 */
public interface TaskExecutorService {

	/**
	 * Executes next task
	 * @param context task context
	 * @return task result or throws exception
	 */
	TaskResult execute(TaskContext context);

	/**
	 * Removes tasks from queue
	 * @param state of tasks to be removed
	 */
	void purge(TaskState state);
}
