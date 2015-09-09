package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.TaskState;

/**
 * Takes care of job execution, utilizes TaskQueueService to get next job
 *
 * 1. retrieves next job from queue and locks it for other executors
 * 2. executes job - takes care of failover (job taking to long and other exceptions)
 * 3. unlocks job when execution finishes
 *
 */
public interface TaskExecutorService {

	/**
	 * Executes next job
	 * @param context job context
	 * @return job result or throws exception
	 */
	TaskResult execute(TaskContext context);

	/**
	 * Removes tasks from queue
	 * @param state of tasks to be removed
	 */
	int purge(TaskState state);
}
