package com.spikeify.taskqueue;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task which executes for at least a minute
 */
public class LongRunningTask implements Job {

	public static final Logger log = LoggerFactory.getLogger(LongRunningTask.class.getSimpleName());

	private long duration;

	private boolean ignore;

	public LongRunningTask() {
		duration = 60L * 1000L;
		ignore = false;
	}

	public LongRunningTask(long executeInMilliseconds) {
		duration = executeInMilliseconds;
		ignore = false;
	}

	public LongRunningTask(long executeInMilliseconds, boolean ignoreInterrupt) {
		duration = executeInMilliseconds;
		ignore = ignoreInterrupt;
	}

	@JsonProperty("duration")
	public long getDuration() {

		return duration;
	}

	@JsonProperty("duration")
	public void setDuration(int duration) {

		this.duration = duration;
	}

	@JsonProperty("ignore")
	public boolean getIgnore() {

		return ignore;
	}

	@JsonProperty("ignore")
	public void setIgnore(boolean ignore) {

		this.ignore = ignore;
	}

	@Override
	public TaskResult execute(TaskContext context) {

		long startTime = System.currentTimeMillis();

		long age;
		do {

			age = System.currentTimeMillis() - startTime;

			try {
				if (context != null && !ignore && context.interrupted()) {

					log.info("Task is being interrupted ... sending kill signal!");
					Thread.sleep(3000); // wait 3 seconds ... simulate that interrupt takes time
					return TaskResult.interrupted(); // gracefully exit
				}

				Thread.sleep(100); // wait 1/10 of a second
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
