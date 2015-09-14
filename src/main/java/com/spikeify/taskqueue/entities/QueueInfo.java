package com.spikeify.taskqueue.entities;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.JsonUtils;

/**
 * Information and statistics about queues
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
	 * true - queue is enabled,
	 * false - queue is disabled
	 */
	protected boolean enabled;

	protected QueueInfo() {
		// for Spikeify
	}

	public QueueInfo(String queueName) {

		Assert.notNullOrEmpty(queueName, "Missing queue name!");
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
}
