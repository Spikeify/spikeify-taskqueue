package com.spikeify.taskqueue;

/**
 *
 */
public interface Job {

	/**
	 * @param context job context - should be regularly checked if "interrupted()" was set as this is a signal to running task to end gracefully,
	 * @return job result indicating job success or failure (should not be null)
	 */
	TaskResult execute(TaskContext context);
}
