package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.QueueTask;

/**
 * Takes care of task execution, utilizes TaskQueueService to get next task
 *
 * 1. retrieves next task from queue and lock it for other executors
 * 2. executes task - takes care of failover (task taking to long and other exceptions)
 * 3. unlocks task when execution finishes
 *
 */
public interface TaskExecutorService {

	QueueTask lock(QueueTask task);

	TaskResult execute(TaskContext context);

	QueueTask unlock(QueueTask task);
}
