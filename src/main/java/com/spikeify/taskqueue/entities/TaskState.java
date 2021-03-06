package com.spikeify.taskqueue.entities;

import java.util.HashMap;
import java.util.Map;

public enum TaskState {

	queued,       // job was added to queue
	running,      // job will be executed by a worker (job is locked for other workers)
	finished,     // job was successfully
	interrupted,  // job was interrupted
	failed,       // job has failed ... it might be retried if run count was not exceeded (job is unlocked)
	purge;        // job can be removed from database ... is marked for deletion

	// possible state transitions
	private static Map<TaskState, TaskState[]> transitionMatrix;

	static {
		transitionMatrix = new HashMap<>();
		transitionMatrix.put(null, new TaskState[] {queued});                	// add
		transitionMatrix.put(queued, new TaskState[] {running, purge});     	// execute
		transitionMatrix.put(running, new TaskState[] {finished, failed, interrupted});    // success, interruption or failure
		transitionMatrix.put(failed, new TaskState[] {running, purge});     	// retry or delete
		transitionMatrix.put(interrupted, new TaskState[] {running, failed, purge});   	// retry or delete
		transitionMatrix.put(finished, new TaskState[] {purge});            	// delete
	}

	public boolean canTransition(TaskState toState) {

		TaskState[] possible = transitionMatrix.get(this);
		if (possible == null) {
			return false; // prevention ... should never happen
		}

		for (TaskState state : possible) {
			if (state.equals(toState)) {
				return true;
			}
		}

		return false;
	}
}
