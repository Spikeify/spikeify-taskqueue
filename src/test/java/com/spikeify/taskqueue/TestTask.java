package com.spikeify.taskqueue;

/**
 * Test task for unit testing
 */
public class TestTask implements Task {

	private String property;

	protected TestTask() {
		// for Jackson
	}

	public TestTask(String value) {
		setProperty(value);
	}

	public String getProperty() {

		return property;
	}

	public void setProperty(String value) {
		property = value;
	}


	@Override
	public TaskResult execute(TaskContext context) {

		// nothing to do right now ...
		return null;
	}
}
