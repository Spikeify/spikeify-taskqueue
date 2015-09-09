package com.spikeify.taskqueue.utils;

import com.spikeify.taskqueue.TaskQueueError;

/**
 * Method parameter check helper - to avoid additional dependencies
 * add additional checks when needed
 */
public final class Assert {

	private Assert() {	}

	public static void notNull(Object test, String message) {

		isFalse(test == null, message);
	}

	public static void isTrue(boolean condition, String message) {
		if (!condition) {
			throw new TaskQueueError(message);
		}
	}

	public static void isFalse(boolean condition, String message) {
		if (condition) {
			throw new TaskQueueError(message);
		}
	}
}
