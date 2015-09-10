package com.spikeify.taskqueue.entities;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;
import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.IdGenerator;
import com.spikeify.taskqueue.utils.JsonUtils;

/**
 * Entity holding a task to be stored/retrieved in/from Aerospike database
 */
public class QueueTask {

	private static final String LOCKED = "LOCKED";
	private static final String OPEN = "OPEN";

	private static final int MAX_RETRIES = 3;

	@Generation
	protected Integer generation;

	/**
	 * unique key assigned to task when added to queue
	 */
	@UserKey
	private String id;

	/**
	 * queue name
	 */
	// @Indexed
	protected String queue;

	/**
	 * to JSON string serialized task
	 */
	protected String job;

	/**
	 * java class name
	 */
	protected String className;

	/**
	 * time stamp when task was created/added to the queue
	 */
	protected long createTime;

	/**
	 * time stamp when task was last updated
	 */
	protected long updateTime;

	/**
	 * time task was started
	 */
	protected long startTime;

	/**
	 * time task finished
	 */
	protected long endTime;

	/**
	 * internal task state ... execution progress
	 */
	protected TaskState state;

	/**
	 * count of taks runs 0 - taks was never run, 1 - task was run once, 2 - run once / and once retried ...
	 * use in combination with state to determine general task state
	 */
	protected int runCount;

	/**
	 * Joined values of QueueName + TaskState to enable filtering
	 */
	@Indexed
	public String stateFilter; // set to private later

	/**
	 * Joined values of QueueName + locked state to enable filtering
	 */
	@Indexed
	public String lockFilter; // set to private

	/**
	 * For Spikeify only
	 */
	protected QueueTask() {

	}

	/**
	 * Creates new queue task entity holding a task to be stored into database
	 *
	 * @param newJob    task to be stored
	 * @param queueName name of queue to put task into
	 */
	public QueueTask(Job newJob, String queueName) {

		Assert.notNull(newJob, "Missing task!");
		Assert.notNull(queueName, "Missing queue name!");

		// generated id ... must check if unique before adding task to queue
		generateId();

		queue = queueName;

		// initial task state ...
		createTime = System.currentTimeMillis();
		updateTime = createTime;
		startTime = 0;
		endTime = 0;

		state = TaskState.queued;
		runCount = 0;

		job = JsonUtils.toJson(newJob);
		className = newJob.getClass().getName();

		updateFilter();
	}

	/**
	 * Creates instance of task object from JSON task and className
	 * (task class must be serializable/deserializable from to JSON string)
	 *
	 * @return Task to be executed ...
	 */
	public Job getJob() {

		try {
			Class clazz = this.getClass().getClassLoader().loadClass(className);
			Object instance = JsonUtils.fromJson(job, clazz);

			// check type
			if (!(instance instanceof Job)) {
				// this is not a "Task" ... so execution would be impossible
				throw new TaskQueueError("Class '" + clazz.getName() + "' must derive from '" + Job.class.getName() + "'!");
			}

			return (Job) instance;
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

	/**
	 * @return unique task id
	 */
	public String getId() {

		return id;
	}

	/**
	 * Create new id if needed
	 * Useful for task duplication or in case id is duplicated in database
	 */
	public void generateId() {

		id = IdGenerator.generateKey();
	}

	/**
	 * @return time stamp task was created
	 */
	public Long getCreateTime() {

		return createTime;
	}

	/**
	 * @return last time task was updated (change of state)
	 */
	public Long getUpdateTime() {

		return updateTime;
	}

	/**
	 * @return current task state
	 */
	public TaskState getState() {

		return state;
	}

	/**
	 * @return name of queue task was put in
	 */
	public String getQueue() {

		return queue;
	}

	/**
	 * Change the tasks state when started, failed of finished
	 *
	 * @param newState to put task into. If transition is not possible set is ignored!
	 */
	public void setState(TaskState newState) {

		if (!state.canTransition(newState)) {
			throw new TaskQueueError("Can't transition from: " + state + " to: " + newState);
		}

		updateTime = System.currentTimeMillis();

		if (TaskState.running.equals(newState)) {
			runCount++;
			startTime = System.currentTimeMillis();
		}

		if (TaskState.finished.equals(newState) ||
			TaskState.failed.equals(newState)) {
			endTime = System.currentTimeMillis();
		}

		state = newState;

		updateFilter();
	}

	/**
	 * @return number of task executions
	 */
	public int getRunCount() {

		return runCount;
	}

	/**
	 * @return true if task is locked (can not be modified)
	 */
	public boolean isLocked() {

		return TaskState.running.equals(state) ||
			   TaskState.finished.equals(state) ||
			   (TaskState.failed.equals(state) &&
				runCount > MAX_RETRIES);
	}

	@Override
	public String toString() {

		return id + ": " + className + " [" + state + "]";
	}

	/**
	 * Filter is composed of queue name and locked state
	 * this allows filtering of tasks per queue and state, for instance:
	 * - get all queued or failed tasks for default queue
	 * - get all running or finished tasks for "my" queue ...
	 */
	protected void updateFilter() {

		lockFilter = getLockedFilter(queue, isLocked());
		stateFilter = getStateFilter(queue, state);
	}


	/**
	 * Utility method to get correct filter for equals filtering searching for open/closed tasks
	 *
	 * @param queueName name of queue
	 * @param locked    state
	 * @return filter expression
	 */
	public static String getLockedFilter(String queueName, boolean locked) {

		return queueName + "::" + (locked ? LOCKED : OPEN);
	}

	/**
	 * Utility method to get correct filter for equals filtering searching for task state
	 *
	 * @param queueName name of queue
	 * @param taskState state
	 * @return filter expression
	 */
	public static String getStateFilter(String queueName, TaskState taskState) {

		return queueName + "::" + taskState.name();
	}
}
