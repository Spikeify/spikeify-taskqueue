package com.spikeify.taskqueue;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.logging.Logger;

/**
 * Task which executes for at least a minute
 */
public class LongRunningTask implements Job {

	public static final Logger log = Logger.getLogger(LongRunningTask.class.getSimpleName());

	private long duration;

	public LongRunningTask() {
		duration = 60L * 1000L;
	}

	public LongRunningTask(long executeInMilliseconds) {
		duration = executeInMilliseconds;
	}

	@JsonProperty("duration")
	public long getDuration() {

		return duration;
	}

	@JsonProperty("duration")
	public void setDuration(int duration) {

		this.duration = duration;
	}

	@Override
	public TaskResult execute(TaskContext context) {

		long startTime = System.currentTimeMillis();

		long age;
		do {

			age = System.currentTimeMillis() - startTime;

			try {
				if (context != null && context.interrupted()) {

					Thread.sleep(3000); // wait 3 seconds ... simulate that interrupt takes time
					return TaskResult.interrupted(); // gracefully exit
				}

				Thread.sleep(100); // wait one second
			}
			catch (InterruptedException e) {
				return TaskResult.interrupted(); // gracefully exit
			}
		}
		while (age <= duration);

		log.info("Long run finished successfully!");
		return TaskResult.ok();
	}
}
