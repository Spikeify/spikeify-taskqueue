package com.spikeify.taskqueue.entities;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.JsonUtils;
import com.spikeify.taskqueue.utils.StringUtils;

import java.util.HashMap;

/**
 * Information and running statistics about queues
 */
public class QueueInfo {

	@UserKey
	protected String name;

	@Generation
	protected int generation;

	/**
	 * JSON serialized {@link QueueSettings}
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

	// field names corresponding to TaskStates in order to count number of tasks in different states
	protected long queued;
	protected long running;
	protected long purge;
	protected long interrupted;
	protected long failed;
	protected long finished;

	// total number of tasks processed
	protected long totalTasks;

	// total number of successful tasks
	protected long totalFinished;

	// total number of failed tasks
	protected long totalFailed;

	// total number of retries - from failed to running transition
	protected long totalRetries;

	/**
	 * JSON serialized map of statistics data {@link TaskState} {@link TaskStatistics}
	 **/
	protected HashMap<TaskState, String> statistics = new HashMap<>();

	protected QueueInfo() {
		// for Spikeify
	}

	public QueueInfo(String queueName) {

		Assert.notNullOrEmpty(queueName, "Missing queue name!");

		// default settings
		enabled = true;
		name = queueName.trim();

		setSettings(new QueueSettings()); // set default settings ... once queue is created
		reset(true);
	}

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

	public void reset(boolean force) {

		// reset total counts
		totalTasks = 0;
		totalFinished = 0;
		totalFailed = 0;
		totalRetries = 0;

		// running counters
		if (force) {
			queued = 0;
			running = 0;
			purge = 0;
			interrupted = 0;
			failed = 0;
			finished = 0;
		}

		// purge statistics if available
		statistics = new HashMap<>();
	}

	public long getQueuedTasks() {

		return queued;
	}

	public long getRunningTasks() {

		return running;
	}

	public long getPurgeTasks() {

		return purge;
	}

	public long getInterruptedTasks() {

		return interrupted;
	}

	public long getFailedTasks() {

		return failed;
	}

	public long getFinishedTasks() {

		return finished;
	}

	public long getTotalTasks() {

		return totalTasks;
	}

	public long getTotalFinished() {

		return totalFinished;
	}

	public long getTotalFailed() {

		return totalFailed;
	}

	public long getTotalRetries() {

		return totalRetries;
	}

	public TaskStatistics getStatistics(TaskState state) {

		String json = statistics.get(state);
		if (StringUtils.isNullOrEmptyTrimmed(json)) {
			return null;
		}

		return JsonUtils.fromJson(json, TaskStatistics.class);
	}

	public void setStatistics(TaskState state, TaskStatistics output) {

		if (state != null && output != null) {

			TaskStatistics old = getStatistics(state);
			if (old != null) {

				TaskStatistics.Builder builder = new TaskStatistics.Builder();
				TaskStatistics joined = builder.include(old).buildWith(output);

				String json = JsonUtils.toJson(joined);
				statistics.put(state, json);
			}
			else {
				String json = JsonUtils.toJson(output);
				statistics.put(state, json);
			}
		}
	}
}
