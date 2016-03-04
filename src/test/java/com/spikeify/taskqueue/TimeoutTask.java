package com.spikeify.taskqueue;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeoutTask implements Job {

	public static final Logger log = LoggerFactory.getLogger(TimeoutTask.class);

	public TimeoutTask() {
		ignore = false;
	}

	public TimeoutTask(boolean ignoreInterrupt) {
		ignore = ignoreInterrupt;
	}

	private boolean ignore;

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

		long duration = 60 * 1000; // 60 seconds
		long startTime = System.currentTimeMillis();

		long age;
		do {

			age = System.currentTimeMillis() - startTime;

			try {
				if (context != null && !ignore && context.interrupted()) {

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
