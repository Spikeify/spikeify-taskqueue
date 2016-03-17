package com.spikeify.taskqueue;

/**
 *
 */
public class ExecutionContext implements TaskContext {

	private final TaskContext outerContext;

	private boolean cancel;

	public ExecutionContext(TaskContext context) {

		outerContext = context;
	}

	@Override
	public boolean interrupted() {

		return cancel || outerContext.interrupted();
	}

	@Override
	public void interrupt() {

		cancel = true;
	}
}
