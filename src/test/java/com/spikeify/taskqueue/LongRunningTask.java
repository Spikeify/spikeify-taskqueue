package com.spikeify.taskqueue;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task which executes for at least a minute
 */
public class LongRunningTask implements Job {

	public static final Logger log = LoggerFactory.getLogger(LongRunningTask.class.getSimpleName());

	private long wait;

	private long duration;

	private boolean ignore;

	private String name;

	private LongRunningTask() {}

	public LongRunningTask(String name) {
		duration = 60L * 1000L;
		ignore = false;
		wait = 3000;
		this.name = name;
	}

	public LongRunningTask(String name, long executeInMilliseconds) {
		this.name = name;
		duration = executeInMilliseconds;
		ignore = false;
		wait = 3000;
	}

	public LongRunningTask(String name, long executeInMilliseconds, boolean ignoreInterrupt) {
		this.name = name;
		duration = executeInMilliseconds;
		ignore = ignoreInterrupt;
		wait = 3000;
	}

	public LongRunningTask(String name, long executeInMilliseconds, long waitUntilInterrupt) {
		this.name = name;
		duration = executeInMilliseconds;
		wait = waitUntilInterrupt;
		ignore = false;
	}

	@JsonProperty("name")
	public String getName() {

		return name;
	}

	@JsonProperty("name")
	public void setName(String value) {

		name = value;
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

	@JsonProperty("wait")
	public long getWait() {

		return wait;
	}

	@JsonProperty("wait")
	public void setWait(long time) {

		wait = time;
	}


	@Override
	public TaskResult execute(TaskContext context) {

		long startTime = System.currentTimeMillis();

		long age;
		do {

			age = System.currentTimeMillis() - startTime;

			try {
				if (context != null && !ignore && context.interrupted()) {

					log.info("Task " + name + " is being interrupted ... sensing interrupt from context!");
					Thread.sleep(wait); // wait 3 seconds ... simulate that interrupt takes time
					log.info("Task " + name + " gracefully ending his execution.");
					return TaskResult.interrupted(); // gracefully exit
				}

				Thread.sleep(100); // wait 1/10 of a second
			}
			catch (InterruptedException e) {

				log.info(name + " being interrupted / kill signal");
				return TaskResult.interrupted(); // gracefully exit
			}
		}
		while (age <= duration);

		log.info(name + " run finished successfully!");
		return TaskResult.ok();
	}
}
