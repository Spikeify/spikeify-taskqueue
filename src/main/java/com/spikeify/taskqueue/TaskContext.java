package com.spikeify.taskqueue;

/**
 * A context bundle containing information about the job execution and environment
 */
public interface TaskContext {

	/**
	 * To be used to send interrupt signal to running task
	 *
	 * @return true in case task should be interrupted, false otherwise
	 */
	boolean interrupted();

	/**
	 * Manual interrupt ... forced ... should set interrupted to true
	 */
	void interrupt();
}
