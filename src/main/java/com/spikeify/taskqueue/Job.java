package com.spikeify.taskqueue;

/**
 *
 */
public interface Job {

	/**
	 * @param context job context
	 * @return job result indicating job success or failure (should not be null)
	 */
	TaskResult execute(TaskContext context);

	/**
	 * Interrupt signal to job
	 * It is open how to implement this in each job, but once this method is called job should try to gracefully exit if possible
	 * The interrupt signal will be send in case a running job is taking to long, or seems to be hanging.
	 */
	void interrupt();
}
