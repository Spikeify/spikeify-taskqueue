package com.spikeify.taskqueue.entities;

import java.util.HashMap;
import java.util.Map;

public enum TaskState {

	queued,     // task was added to queue
	running,    // task will be executed by a worker (task is locked for other workers)
	finished,   // task was successfully
	failed;     // task has failed ... it might be retried if run count was not exceeded (task is unlocked)

	// possible state transitions
	private static Map<TaskState, TaskState[]> transitionMatrix;

	static {
		transitionMatrix = new HashMap<>();
		transitionMatrix.put(null, new TaskState[] {queued});      // add
		transitionMatrix.put(queued, new TaskState[] {running});   // execute
		transitionMatrix.put(running, new TaskState[] {finished, failed}); // success or failure
		transitionMatrix.put(failed, new TaskState[] {running});   // retry
	}

	public boolean canTransition(TaskState toState) {

		TaskState[] possible = transitionMatrix.get(this);
		if (possible == null) {
			return false;
		}

		for (TaskState state : possible) {
			if (state.equals(toState)) {
				return true;
			}
		}

		return false;
	}
}
