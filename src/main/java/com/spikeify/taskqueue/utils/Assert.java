package com.spikeify.taskqueue.utils;

/**
 * Method parameter check helper - to avoid additional dependencies
 * add additional check when needed
 */
public final class Assert {

	private Assert() {	}

	public static void notNull(Object test, String message) {

		if (test == null) {
			throw new IllegalArgumentException(message);
		}
	}
}
