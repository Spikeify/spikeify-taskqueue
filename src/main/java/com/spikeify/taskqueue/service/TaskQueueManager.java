package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.QueueSettings;

import java.util.List;

public interface TaskQueueManager {

	/**
	 * Registers queue to be monitored
	 *
	 * @param queueName name of queue
	 * @param autoStart true will put queue into started mode (once check is called queue will be started), false queue must be put into started mode manually
	 * @return registered queue info
	 */
	QueueInfo register(String queueName, boolean autoStart);

	QueueInfo register(String queueName, QueueSettings settings, boolean autoStart);

	/**
	 * Returns single queue info with statistics
	 *
	 * @param queueName name of queue
	 * @return queue info
	 */
	QueueInfo info(String queueName);

	/**
	 * Will reset all statistics data and counts
	 *
	 * @param queueName name of queue
	 * @param force true will reset also running task counters (running, queued ... etc) might result in negative counters!
	 */
	void resetStatistics(String queueName, boolean force);

	/**
	 * Lists queues registered
	 *
	 * @param active - true list active queues, false - list disabled queues, null - list all
	 * @return list of registered queues
	 */
	List<QueueInfo> list(Boolean active);

	/**
	 * Removes queue from monitoring
	 *
	 * @param queueName to be removed
	 */
	void unregister(String queueName);

	/**
	 * Starts queues ... (on given JVM)
	 * should be called only once per thread,
	 * if called multiple times then threads are terminated and restated (acts as restart)
	 *
	 * @param queueNames names to be started or empty to start all enabled queues
	 * @throws InterruptedException when interrupted
	 */
	void start(String... queueNames) throws InterruptedException;

	/**
	 * Returns instance of task executor service to be used when executing jobs
	 * @param queueName name of queue
	 * @return executor service
	 */
	TaskExecutorService getExecutor(String queueName);

	/**
	 * Stops queues - stops all running tasks/threads (on given JVM)
	 *
	 * @param queueNames to be stopped or empty to stop all enabled queues
	 * @throws InterruptedException when interrupted
	 */
	void stop(String... queueNames) throws InterruptedException;

	/**
	 * @param queueName name of queue
	 * @return true if queue is started and active (running), false if not
	 */
	boolean isRunning(String queueName);

	/**
	 * Enables queue
	 *
	 * @param queueName - enables queue to be run
	 * @return enabled queue info
	 */
	QueueInfo enable(String queueName);

	/**
	 * Disables queue - stops queue if running
	 *
	 * @param queueName - disables queue from running
	 * @return disabled queue info
	 */
	QueueInfo disable(String queueName);

	/**
	 * Should be called on regular basis from each machine running queues
	 * takes care that if one instance has started/stopped a queue it is also started/stopped on other machines
	 * <p>
	 * Best invoked from a cron job or similar
	 *
	 * @param queueNames list of queues to check / null or empty to check them all
	 * @throws InterruptedException when interrupted
	 */
	void check(String... queueNames) throws InterruptedException;

	/**
	 * Sets queue settings
	 *
	 * @param queue    queue name
	 * @param settings to be stored
	 */
	void set(String queue, QueueSettings settings);
}
