package com.spikeify.taskqueue;

public enum TaskPriority {

	normal, // default task execution priority
	low, 	// lower priority, task can wait if higher priority task are in the queue
	urgent  // higher priority, task should be executed as fast as possible
}
