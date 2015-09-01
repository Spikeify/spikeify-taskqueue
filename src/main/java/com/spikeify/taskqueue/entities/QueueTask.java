package com.spikeify.taskqueue.entities;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.TaskPriority;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.utils.Assert;
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
	private String key;

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
	protected Long created;

	/**
	 * time stamp when task was last updated
	 */
	protected Long updated;

	/**
	 * task execution priority
	 */
	protected TaskPriority priority;

	/**
	 * internal task state ... execution progress
	 */
	protected TaskState state;

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

		this(job, TaskPriority.normal);
	}

	/**
	 * Creates new queue task entity holding a task to be stored into database
	 *
	 * @param job         task to be stored
	 * @param jobPriority job priority
	 */
	public QueueTask(Task job, TaskPriority jobPriority) {

		Assert.notNull(job, "Missing task!");
		Assert.notNull(jobPriority, "Missing task priority!");

		created = System.currentTimeMillis();
		updated = created;

		priority = jobPriority;
		state = TaskState.queued;
		runCount = 0;

		task = JsonUtils.toJson(job);
		className = job.getClass().getName();
	}

	/**
	 * Creates instance of task object from JSON task and className (class name)
	 *
	 * @return
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
			// JSON deserializaton problem
			throw new TaskQueueError("Deserialization problem: " + e.getMessage());
		}
		catch (ClassNotFoundException e) {
			// class can't be found ...
			throw new TaskQueueError("Class '" + className + "' not found!", e);
		}
	}
}
