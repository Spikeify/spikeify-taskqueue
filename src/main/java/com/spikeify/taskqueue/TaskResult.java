package com.spikeify.taskqueue;

/**
 * Task execution result
 */
public interface TaskResult {

	/**
	 * @return true if result was successful, false if task failed and should be retried
	 */
	boolean isOK();
}
