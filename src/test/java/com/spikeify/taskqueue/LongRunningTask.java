package com.spikeify.taskqueue;

/**
 * Task which executes for at least a minute
 */
public class LongRunningTask implements Job {

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

				Thread.sleep(1000); // wait one second
			}
			catch (InterruptedException e) {
				return TaskResult.interrupted(); // gracefully exit
			}
		}
		while (age < 60L * 1000L);

		return TaskResult.ok();
	}
}
