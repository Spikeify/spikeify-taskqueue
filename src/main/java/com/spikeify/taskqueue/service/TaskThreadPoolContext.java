package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Thread context for job execution ... holds thread information
 */
public class TaskThreadPoolContext implements TaskContext {

	private final ScheduledThreadPoolExecutor executor;

	private boolean timeOut;

	public TaskThreadPoolContext(ScheduledThreadPoolExecutor executorService) {

		executor = executorService;
	}

	@Override
	public boolean interrupted() {

		return timeOut || executor.isTerminating();
	}

	@Override
	public void interrupt() {
		timeOut = true;
	}

	public void reset() {
		timeOut = false;
	}
}
