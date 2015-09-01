package com.spikeify.taskqueue;

/**
 *
 */
public interface Task {

	TaskResult execute(TaskContext context);
}
