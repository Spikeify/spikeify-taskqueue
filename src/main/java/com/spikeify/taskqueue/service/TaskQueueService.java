package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.entities.TaskStatistics;

import java.util.List;

/**
 * Adds and retrieves tasks from queue
 */
public interface TaskQueueService {

	/**
	 * Adds job to queue
	 *
	 * @param job       to be executed
	 * @param queueName name of queue
	 * @return added job
	 */
	QueueTask add(Job job, String queueName);

	/**
	 * Gets next job to be executed (put in running state)
	 *
	 * @param queueName name of queue
	 * @return job to be executed or null if no job found
	 */
	QueueTask next(String queueName);

	/**
	 * Lists all tasks from queue in given state
	 *
	 * @param state     job is in
	 * @param queueName name of queue
	 * @return list of tasks in state from queue
	 */
	List<QueueTask> list(TaskState state, String queueName);

	/**
	 * Transitions job from current state to new state
	 * This is done in transaction so only one thread can change the state (this is then the worker thread for this job)
	 *
	 * @param task  to transition state
	 * @param state to transition to
	 * @return updated task if transition successed, false if job could not be transitioned
	 */
	QueueTask transition(QueueTask task, TaskState state);

	/**
	 * Removes tasks from queue
	 *
	 * @param state     of tasks to be removed
	 * @param taskAge   age in minutes to allow action, 0 == now or never
	 * @param queueName name of queue
	 * @return task statistics of removed tasks
	 */
	TaskStatistics purge(TaskState state, int taskAge, String queueName);
}
