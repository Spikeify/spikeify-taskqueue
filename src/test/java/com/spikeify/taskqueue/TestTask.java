package com.spikeify.taskqueue;

/**
 * Test job for unit testing
 */
public class TestTask implements Job {

	private int property;

	protected TestTask() {
		// for Jackson
	}

	public TestTask(int value) {
		setProperty(value);
	}

	public int getProperty() {

		return property;
	}

	public void setProperty(int value) {
		property = value;
	}


	@Override
	public TaskResult execute(TaskContext context) {

		// nothing to do right now ...
		if (context != null && context.interrupted()) {
			return TaskResult.interrupted();
		}

		return TaskResult.ok();
	}
}
