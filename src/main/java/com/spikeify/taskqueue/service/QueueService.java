package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.QueueSettings;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;

import java.util.List;

/**
 *
 */
public interface QueueService {

	/**
	 * Adds new job to queue
	 * @param newJob to be added
	 * @param queueName name of queue
	 * @return task job has been assigned
	 */
	QueueTask addJob(Job newJob, String queueName);

	/**
	 * Lists jobs in certain state
	 * @param state job is in
	 * @param queueName name of queue
	 * @return found list of jobs or empty list if none found
	 */
	List<QueueTask> listJobs(TaskState state, String queueName);

	/**
	 * Changes queue settings and restarts running queue
	 * @param settings to be used
	 * @param queues list of queues or empty for all
	 */
	void settings(QueueSettings settings, String... queues);

	/**
	 * Latest queue info with statistics
	 * @param queueName name of queue
	 * @return info
	 */
	QueueInfo info(String queueName);

	/**
	 * Resets queue statistics
	 * @param queueName name of queue
	 * @return info
	 */
	QueueInfo resetStatistics(String queueName);

	/**
	 * Starts given queues or all if none given
	 * @param queues to be started
	 */
	void start(String... queues);

	/**
	 * Restars queue if running
	 * @param queueName name of queue
	 */
	void restart(String queueName);

	/**
	 * Stops given queues or all if none given
	 * @param queues to be stopped
	 */
	void stop(String... queues);

	/**
	 * Queue check (should be called on regular basis on each machine, JVM running the QueueService)
	 * makes sure queues are started, stopped on each machine
	 */
	void check();
}
