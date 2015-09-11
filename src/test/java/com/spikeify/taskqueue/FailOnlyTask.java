package com.spikeify.taskqueue;

/**
 * This task always fails ...
 */
public class FailOnlyTask implements Job {

	public FailOnlyTask() {}

	@Override
	public TaskResult execute(TaskContext context) {

		return TaskResult.failed();
	}

	@Override
	public void interrupt() {

	}
}
