package com.spikeify.taskqueue;

/**
 * This task always fails ...
 */
public class FailOnlyTask implements Job {

	@Override
	public TaskResult execute(TaskContext context) {

		return TaskResult.failed();
	}
}
