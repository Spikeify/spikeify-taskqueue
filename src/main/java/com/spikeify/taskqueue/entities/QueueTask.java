package com.spikeify.taskqueue.entities;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.utils.JsonUtils;

/**
 * Entity to store tasks in Aerospike database
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
	private String task;

	/**
	 * java class name
	 */
	private String className;


	/**
	 * time stamp when task was created/added to the queue
	 */
	protected Long created;

	/**
	 * time stamp when task was last updated
	 */
	protected Long updated;

	protected QueueTask() {

	}

	/**
	 * Creates new queue task entity holding a task to be stored into database
	 *
	 * @param job task to be stored
	 */
	public QueueTask(Task job) {

		created = System.currentTimeMillis();
		updated = created;

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

			if (!clazz.isInstance(Task.class)) {
				throw new TaskQueueError("Class: " + clazz.getName() + " must derive from Task!");
			}

			return (Task) JsonUtils.fromJson(task, clazz);
		}
		catch (ClassNotFoundException e) {
			throw new TaskQueueError("Class: " + className + " not found!", e);
		}
	}
}
