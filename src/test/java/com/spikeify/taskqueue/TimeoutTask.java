package com.spikeify.taskqueue;

import java.util.logging.Logger;

public class TimeoutTask implements Job {

	public static final Logger log = Logger.getLogger(TimeoutTask.class.getSimpleName());

	@Override
	public TaskResult execute(TaskContext context) {

		long duration = 60 * 1000; // 60 seconds
		long startTime = System.currentTimeMillis();

		long age;
		do {

			age = System.currentTimeMillis() - startTime;

			try {
				if (context != null && context.interrupted()) {

					return TaskResult.interrupted(); // gracefully exit
				}

				Thread.sleep(100); // wait 1/10 of a second
			}
			catch (InterruptedException e) {
				// don't do anything ... task should be killed by force
				log.info("... BANG ... TASK GOT KILLED ... ");
			}
		}
		while (age <= duration);

		return TaskResult.ok();
	}
}
