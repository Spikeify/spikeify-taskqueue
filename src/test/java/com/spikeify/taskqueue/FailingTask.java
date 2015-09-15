package com.spikeify.taskqueue;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class FailingTask implements Job {

	public int number;

	@JsonIgnore
	private boolean interrupted;

	protected FailingTask() {}

	public FailingTask(int taskNumber) {
		number = taskNumber;
	}

	@Override
	public TaskResult execute(TaskContext context) {

		// will fail if context can divide number
		int value = 0;
		if (context instanceof TestTaskContext) {
			value = ((TestTaskContext) context).count;
		}

		// random generated value from 1-100 (10% should fail)
		if (value % 20 == 0) {
			throw new IllegalArgumentException("I'm evil!");
		}

		if (context != null && context.interrupted()) {
			return TaskResult.interrupted();
		}

		return TaskResult.ok();

	}
}
