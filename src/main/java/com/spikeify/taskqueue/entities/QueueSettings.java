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
	protected int purgeSuccessfulAfterMinutes = 0;

	/**
	 * Will trigger purge of failed tasks that are older than given number of minutes
	 * 0 - will not purge tasks
	 */
	protected int purgeFailedAfterMinutes = 0;

	/**
	 * Max number of task retries before failing
	 */
	protected int maxNumberOfRetries = 3;

	/**
	 * Number of minutes task is considered hanged or execution takes to long
	 * and interrupt is send ... than task is put into failed state
	 */
	protected int taskTimeoutMinutes = 10;


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

	@JsonProperty("maxRetries")
	public int getMaxNumberOfRetries() {

		return maxNumberOfRetries;
	}

	@JsonProperty("maxRetries")
	public void setMaxNumberOfRetries(int max) {

		Assert.isTrue(max >= 0, "Number of retries must be >= 0!");
		Assert.isTrue(max <= 10, "Number of retries must be <= 10!");
		maxNumberOfRetries = max;
	}

	@JsonProperty("timeout")
	public void setTaskTimeoutMinutes(int minutes) {
		Assert.isTrue(minutes > 0, "Time out must be >= 1!");
		Assert.isTrue(minutes <= 60, "Time out must be <= 60!");

		taskTimeoutMinutes = minutes;
	}

	@JsonProperty("timeout")
	public int getTaskTimeoutMinutes() {
		return taskTimeoutMinutes;
	}
}
