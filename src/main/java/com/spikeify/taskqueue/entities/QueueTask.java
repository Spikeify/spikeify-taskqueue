package com.spikeify.taskqueue.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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

	private static final ObjectMapper jsonMapper = new ObjectMapper();

	static {
		jsonMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
	}

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
	 * Execution time ... from creation until successful or failed finish
	 * is null when not finished jet (also when failed)
	 * is != null when finished or failed (locked)
	 */
	protected Long executionTime;

	/**
	 * Time job took to execute (pure job execution time) from: running to: success/fail
	 * is null when not run
	 * is != null if run (shows last execution run time)
	 */
	protected Long jobRunTime;

	/**
	 * internal task state ... execution progress
	 */
	protected TaskState state;

	/**
	 * count of tasks runs 0 - tasks was never run, 1 - task was run once, 2 - run once / and once retried ...
	 * use in combination with state to determine general task state
	 */
	protected int runCount;

	/**
	 * Joined values of QueueName + TaskState to enable filtering
	 */
	@Indexed
	protected String stateFilter;

	/**
	 * Joined values of QueueName + locked state to enable filtering
	 */
	@Indexed
	protected String lockFilter;

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

		job = JsonUtils.toJson(newJob, jsonMapper);
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

			Object instance = JsonUtils.fromJson(job, clazz, jsonMapper);

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
	 * @return class executing the task
	 */
	public String getClassName() {

		return className;
	}

	/**
	 * @return time task has been started
	 */
	public long getStartTime() {

		return startTime;
	}

	/**
	 * @return time task has ended / succesfully or failed
	 */
	public long getEndTime() {

		return endTime;
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
	 * @return null or duration in milliseconds job took to execute
	 */
	public Long getJobRunTime() {

		return jobRunTime;
	}

	/**
	 * @return null or duration in milliseconds task took from being added to the queue till finished
	 */
	public Long getExecutionTime() {

		return executionTime;
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

		// log last update
		updateTime = System.currentTimeMillis();

		// log start time and increase run attempts
		if (TaskState.running.equals(newState)) {
			startTime = System.currentTimeMillis();
			runCount++;
		}

		// log end time
		if (TaskState.finished.equals(newState) ||
			TaskState.failed.equals(newState)) {
			endTime = System.currentTimeMillis();

			// execution statistics
			jobRunTime = endTime - startTime;
			executionTime = endTime - createTime;
		}

		// set state
		state = newState;

		// update filtering
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
			   TaskState.purge.equals(state) ||
			   (TaskState.failed.equals(state) && runCount >= MAX_RETRIES);
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

	/**
	 * Compares last update time with current time ...
	 *
	 * @param ageInMinutes number of minutes to pass (age)
	 * @return true if task is older than given minutes, false otherwise
	 */
	public boolean isOlderThan(int ageInMinutes) {

		if (ageInMinutes == 0) {
			return true;
		}

		long difference = System.currentTimeMillis() - updateTime;
		return difference >= ((long) ageInMinutes * 1000L * 60L);
	}

	/**
	 * Compares last update time with current time ...
	 *
	 * @param ageInSeconds number of seconds to pass (age)
	 * @return true if task is older than given seconds, false otherwise
	 */
	public boolean isOlderThanSeconds(int ageInSeconds) {

		if (ageInSeconds == 0) {
			return true;
		}

		long difference = System.currentTimeMillis() - updateTime;
		return difference >= ((long) ageInSeconds * 1000L);
	}
}
