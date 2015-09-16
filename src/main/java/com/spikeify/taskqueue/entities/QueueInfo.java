package com.spikeify.taskqueue.entities;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.JsonUtils;

/**
 * Information and running statistics about queues
 */
public class QueueInfo {

	@UserKey
	protected String name;

	@Generation
	protected int generation;

	/**
	 * JSON serialized QueueSettings
	 */
	protected String queueSettings;

	/**
	 * true - queue is enabled - can be started and stopped,
	 * false - queue is disabled - can not be started or stopped
	 */
	protected boolean enabled;

	/**
	 * true - queue is started (is running or should be running)
	 * false - queue is stopped (is not running or shouldn't be running)
	 */
	protected boolean started;

	protected QueueInfo() {
		// for Spikeify
	}

	public QueueInfo(String queueName) {

		Assert.notNullOrEmpty(queueName, "Missing queue name!");

		// default settings
		enabled = true;
		name = queueName.trim();

		setSettings(new QueueSettings());
	}
/*

	*/
	/**
	 * Queued tasks in queue
	 *//*

	protected long queuedTasks;

	*/
	/**
	 * Currently running tasks
	 *//*

	protected long runningTasks;

	*/
	/**
	 * Failed task
	 *//*

	protected long failedTasks;

	*/
	/**
	 * Successfully completed tasks
	 *//*

	protected long completedTasks;

	*/
	/**
	 * Average task execution duration in ms
	 *//*

	protected long executionDuration;

	*/

	/**
	 * Average task waiting time in ms
	 *//*

	protected long waitingDuration;
*/
	public String getName() {

		return name;
	}

	public QueueSettings getSettings() {

		return JsonUtils.fromJson(queueSettings, QueueSettings.class);
	}

	public void setSettings(QueueSettings settings) {

		queueSettings = JsonUtils.toJson(settings);
	}

	public boolean isEnabled() {

		return enabled;
	}

	public void setEnabled(boolean active) {

		enabled = active;
	}

	public boolean isStarted() {

		return started;
	}

	public void setStarted(boolean start) {

		started = start;
	}
}
