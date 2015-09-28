package com.spikeify.taskqueue.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spikeify.taskqueue.utils.Assert;

/**
 * JSON serialized list of queue settings
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueueSettings {

	/**
	 * Will purge successful tasks after completion when older than given number of minutes
	 * 0 - will purge tasks immediately
	 */
	protected int purgeSuccessfulAfterMinutes = 10;

	/**
	 * Will trigger purge of failed tasks that are older than given number of minutes
	 * 0 - will not purge tasks
	 */
	protected int purgeFailedAfterMinutes = 30;

	/**
	 * Number of minutes task is considered hanged or execution takes to long
	 * and interrupt is send ... than task is put into failed state
	 */
	protected int taskTimeoutSeconds = 10 * 60;

	/**
	 * Maximal number of threads for this queue on a single machine
	 */
	protected int maxThreads = 1;

	/**
	 * Number of seconds queue sleeps to check if new tasks have arrived
	 */
	private long queueMaxSleepTime = 10;

	/**
	 * Number of seconds purge sleeps before it is triggered (execute every 60 seconds)
	 */
	private long queuePurgeSleepTime = 60;


	@JsonProperty("purgeSuccessful")
	public int getPurgeSuccessfulAfterMinutes() {

		return purgeSuccessfulAfterMinutes;
	}

	@JsonProperty("purgeSuccessful")
	public void setPurgeSuccessfulAfterMinutes(int minutes) {

		Assert.isTrue(minutes >= 0, "Number of minutes must be >= 0!");
		purgeSuccessfulAfterMinutes = minutes;
	}

	@JsonProperty("purgeFailed")
	public int getPurgeFailedAfterMinutes() {

		return purgeFailedAfterMinutes;
	}

	@JsonProperty("purgeFailed")
	public void setPurgeFailedAfterMinutes(int minutes) {

		Assert.isTrue(minutes >= 0, "Number of minutes must be >= 0!");
		purgeFailedAfterMinutes = minutes;
	}

	@JsonProperty("timeout")
	public void setTaskTimeoutSeconds(int seconds) {
		Assert.isTrue(seconds > 0, "Time out must be >= 1!");
		Assert.isTrue(seconds <= (60 * 60), "Time out must be <= 3600!");

		taskTimeoutSeconds = seconds;
	}

	@JsonProperty("timeout")
	public int getTaskTimeoutSeconds() {
		return taskTimeoutSeconds;
	}

	@JsonProperty("threads")
	public int getMaxThreads() {
		return maxThreads;
	}

	@JsonProperty("threads")
	public void setMaxThreads(int threads) {

		Assert.isTrue(threads > 0, "Number of threads must be >= 1!");
		Assert.isTrue(threads <= 10, "Number of threads must be <= 10!");

		maxThreads = threads;
	}

	@JsonProperty("checkTasks")
	public long getQueueMaxSleepTime() {

		return queueMaxSleepTime;
	}

	@JsonProperty("checkTasks")
	public void setQueueMaxSleepTime(long queueMaxSleepTime) {

		Assert.isTrue(queueMaxSleepTime > 0, "Number of threads must be >= 1!");
		Assert.isTrue(queueMaxSleepTime <= 600, "Number of threads must be <= 600!");

		this.queueMaxSleepTime = queueMaxSleepTime;
	}

	@JsonProperty("purgeTasks")
	public long getQueuePurgeSleepTime() {

		return queuePurgeSleepTime;
	}

	@JsonProperty("purgeTasks")
	public void setQueuePurgeSleepTime(long queuePurgeSleepTime) {

		Assert.isTrue(queueMaxSleepTime > 0, "Number of threads must be >= 1!");
		Assert.isTrue(queueMaxSleepTime <= 600, "Number of threads must be <= 600!");

		this.queuePurgeSleepTime = queuePurgeSleepTime;
	}
}
