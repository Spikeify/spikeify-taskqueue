package com.spikeify.taskqueue;

/**
 *
 */
public interface Task {

	/**
	 * @param context task context
	 * @return task result indicating task success or failure (should not be null)
	 */
	TaskResult execute(TaskContext context);

	/**
	 * Interrupt signal to task
	 * It is open how to implement this in each task, but once this method is called task should try to gracefully exit if possible
	 * The interrupt signal will be send in case a running task is taking to long, or seems to be hanging.
	 */
	void interrupt();
}
