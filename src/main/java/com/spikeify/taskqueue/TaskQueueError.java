package com.spikeify.taskqueue;

/**
 * Task queue execution exception
 */
public class TaskQueueError extends RuntimeException {

	public TaskQueueError(String message) {
		super(message);
	}

	public TaskQueueError(Exception originalException) {
		super(originalException);
	}

	public TaskQueueError(String message, Exception originalException) {
		super(message, originalException);
	}
}
