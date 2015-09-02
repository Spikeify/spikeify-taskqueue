package com.spikeify.taskqueue.entities;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.IdGenerator;
import com.spikeify.taskqueue.utils.JsonUtils;

/**
 * Entity holding a task to be stored/retrieved in/from Aerospike database
 */
public class QueueTask {

	@Generation
	protected Integer generation;

	/**
	 * unique key assigned to task when added to queue
	 */
	@UserKey
	private String id;

	/**
	 * to JSON string serialized task
	 */
	protected String task;

	/**
	 * java class name
	 */
	protected String className;

	/**
	 * time stamp when task was created/added to the queue
	 */
	protected long created;

	/**
	 * time stamp when task was last updated
	 */
	protected long updated;

	/**
	 * internal task state ... execution progress
	 */
	protected TaskState state;

	/**
	 * true - locked for other threads, false - open for a executor to execute
	 */
	// @Indexed
	protected boolean locked;

	/**
	 * count of taks runs 0 - taks was never run, 1 - task was run once, 2 - task was retried once ...
	 * use in combination with state to determine general task state
	 */
	protected int runCount;

	/**
	 * For Spikeify only
	 */
	protected QueueTask() {

	}

	/**
	 * Creates new queue task entity holding a task to be stored into database
	 *
	 * @param job task to be stored
	 */
	public QueueTask(Task job) {

		Assert.notNull(job, "Missing task!");

		// generated id ... must check if unique before adding task to queue
		generateId();

		created = System.currentTimeMillis();
		updated = created;

		state = TaskState.queued;
		runCount = 0;

		task = JsonUtils.toJson(job);
		className = job.getClass().getName();
	}

	/**
	 * Creates instance of task object from JSON task and className
	 * (task class must be serializable/deserializable from to JSON string)
	 *
	 * @return Task to be executed ...
	 */
	public Task getTask() {

		try {
			Class clazz = this.getClass().getClassLoader().loadClass(className);
			Object instance = JsonUtils.fromJson(task, clazz);

			// check type
			if (!(instance instanceof Task)) {
				// this is not a "Task" ... so execution would be impossible
				throw new TaskQueueError("Class '" + clazz.getName() + "' must derive from '" + Task.class.getName() + "'!");
			}

			return (Task) instance;
		}
		catch (IllegalArgumentException e) {
			// JSON deserialization problem
			throw new TaskQueueError("Deserialization problem: " + e.getMessage());
		}
		catch (ClassNotFoundException e) {
			// class can't be found ...
			throw new TaskQueueError("Class '" + className + "' not found!", e);
		}
	}

	public String getId() {

		return id;
	}

	/**
	 * Create new id if needed
	 * Usefull for task duplication or in case id is duplicated in database
	 */
	public void generateId() {

		id = IdGenerator.generateKey();
	}

	public Long getCreated() {

		return created;
	}

	public Long getUpdated() {

		return updated;
	}

	public TaskState getState() {

		return state;
	}

	public void setState(TaskState newState) {

		if (state.canTransition(newState)) {
			state = newState;

			locked = TaskState.running.equals(newState) ||
					 TaskState.finished.equals(newState);
		}
	}

	public int getRunCount() {

		return runCount;
	}

	@Override
	public String toString() {

		return id + ": " + className + " [" + state + "]";
	}
}
