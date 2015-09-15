package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.TaskContext;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Thread context for job execution ... holds thread information
 */
public class TaskThreadPoolContext implements TaskContext {

	private final ScheduledThreadPoolExecutor executor;

	public TaskThreadPoolContext(ScheduledThreadPoolExecutor executorService) {

		executor = executorService;
	}

	@Override
	public boolean interrupted() {

		return executor.isTerminating();
	}
}
