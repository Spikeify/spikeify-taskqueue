package com.spikeify.taskqueue;

import com.spikeify.taskqueue.entities.TaskResultState;

/**
 * Task execution result
 */
public class TaskResult {

	// state job was in when finished ...
	private final TaskResultState state;

	// context to carry around if necessary
	private final TaskContext context;

	public TaskResult(TaskResultState state) {

		this(state, null);
	}

	public TaskResult(TaskResultState state, TaskContext context) {
		this.state = state;
		this.context = context;
	}

	public TaskResultState getState() {

		return state;
	}

	public TaskContext getContext() {
		return context;
	}

	@Override
	public String toString() {
		return "State: " + state +
			   (context != null ? ", Context: " + context : "");
	}

	// Task result state initializer ... for easier usage

	public static TaskResult ok() {
		return new TaskResult(TaskResultState.ok);
	}

	public static TaskResult failed() {
		return new TaskResult(TaskResultState.failed);
	}

	public static TaskResult interrupted() {
		return new TaskResult(TaskResultState.interrupted);
	}
}
