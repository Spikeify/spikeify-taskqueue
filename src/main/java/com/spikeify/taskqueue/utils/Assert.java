package com.spikeify.taskqueue.utils;

import com.spikeify.taskqueue.TaskQueueError;

/**
 * Method parameter check helper - to avoid additional dependencies
 * add additional checks when needed
 */
public final class Assert {

	private Assert() {	}

	public static void notNull(Object test, String message) {

		if (test == null) {
			throw new TaskQueueError(message);
		}
	}
}
