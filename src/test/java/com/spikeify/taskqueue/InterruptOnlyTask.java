package com.spikeify.taskqueue;

public class InterruptOnlyTask implements Job {

	@Override
	public TaskResult execute(TaskContext context) {

		return TaskResult.interrupted();
	}
}

