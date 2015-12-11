package com.spikeify.taskqueue.service;

import com.aerospike.client.AerospikeException;
import com.spikeify.ResultSet;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyService;
import com.spikeify.Work;
import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.entities.TaskStatistics;
import com.spikeify.taskqueue.utils.Assert;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultTaskQueueService implements TaskQueueService {

	private static final Logger log = Logger.getLogger(DefaultTaskQueueService.class.getSimpleName());

	/**
	 * default queue name if no queue name was given
	 */
	public static final String DEFAULT_QUEUE_NAME = "default";

	/**
	 * number of retries when choosing next taks and task is already taken (by other thread)
	 */
	private static final int CHOOSE_NEXT_TASK_RETRIES = 10;

	/**
	 * number of items to take into consideration from top of the list when choosing next task
	 */
	private static final int MAX_TOP_ITEMS = 5;

	private final Spikeify sfy;

	public DefaultTaskQueueService(Spikeify spikeify) {

		Assert.notNull(spikeify, "Missing spikeify!");
		sfy = spikeify;

		// create indexes if not already present ...
		SpikeifyService.register(QueueTask.class);
	}

	@Override
	public QueueTask add(Job job, String queueName) {

		Assert.notNull(job, "Missing job!");
		Assert.notNullOrEmpty(queueName, "Missing queue name!");

		QueueTask task = new QueueTask(job, queueName);
		sfy.create(task).now();
		setQueueInfoCount(queueName, null, TaskState.queued);

		// create id ... add job ...
		return task;
	}

	/**
	 * Returns next open and put's it into running state
	 * ... might be that in the mean time some other thread will try lock this job
	 * ... so multiple calls to this method are possible
	 *
	 * @param queueName name of queue
	 * @return found job or null if none found
	 */
	@Override
	public QueueTask next(String queueName) {

		Assert.notNullOrEmpty(queueName, "Missing queue name!");

		// note: query can return task that are not open anymore ... so choosing random task it ensures that tasks are distributed more or less evenly among workers
		ResultSet<QueueTask> openTasks = sfy.query(QueueTask.class)
											.filter("lockFilter", QueueTask.getLockedFilter(queueName, false))
											.now();

		// Choose random job ... not the first one
		List<QueueTask> list = openTasks.toList();
		if (list.size() == 0) {
			return null;
		}

		// sort by updateTime ... the older task are on top ...
		// this tries to make sure earlier tasks are executed before later tasks inserted into queue but it is not 100%
		// so no one should rely on this fact
		Collections.sort(list, new Comparator<QueueTask>() {
			@Override
			public int compare(QueueTask o1, QueueTask o2) {

				return o1.getUpdateTime().compareTo(o2.getUpdateTime());
			}
		});

		int size = Math.min(MAX_TOP_ITEMS, list.size()); // 10 or less random from list
		QueueTask proposed = null;

		// try to find open task ...
		for (int i = 1; i <= CHOOSE_NEXT_TASK_RETRIES; i++) {
			Random rand = new Random();
			int idx = rand.nextInt(size);

			proposed = (transition(list.get(idx), TaskState.running));

			if (proposed != null && TaskState.running.equals(proposed.getState())) {
				return proposed;
			}

			size = Math.min(10 * i, list.size()); // make random choice wider
		}

		// last resort ... (don't return null as null is the signal that there are no new tasks)
		return proposed;
	}

	@Override
	public List<QueueTask> list(TaskState state, String queueName) {

		Assert.notNull(state, "Missing job state!");
		Assert.notNullOrEmpty(queueName, "Missing queue name!");

		ResultSet<QueueTask> query = sfy.query(QueueTask.class)
										.filter("stateFilter", QueueTask.getStateFilter(queueName, state))
										.now();
		return query.toList();
	}

	@Override
	public QueueTask transition(QueueTask task, TaskState newState) {

		Assert.notNull(task, "Missing job!");
		Assert.notNull(newState, "Missing state!");

		try {
			// Transition state task
			final String taskId = task.getId();
			final long updateTime = task.getUpdateTime();

			QueueTask updated = sfy.transact(1, new Work<QueueTask>() {
				@Override
				public QueueTask run() {

					// get latest version
					QueueTask original = sfy.get(QueueTask.class).key(taskId).now();

					if (original == null) {
						return null;
					}

					// will throw exception in case transition is not possible (we don't have the latest version from database)
					if (!original.getUpdateTime().equals(updateTime)) {
						throw new TaskQueueError("Thread collision, some other thread already modified task!");
					}

					original.setState(newState);

					// update state ...
					sfy.update(original).now();

					return original;
				}
			});

			// change queue info count
			if (updated != null) {
				setQueueInfoCount(updated.getQueue(), task.getState(), newState);
			}

			return updated;
		}
		catch (ConcurrentModificationException | AerospikeException e) {
			// job modified by other thread ... transition failed
			log.fine("Could not transition job: " + task + " to: " + newState + ", thread collision!");
			return null;
		}
		catch (TaskQueueError e) {

			log.log(Level.SEVERE, "Transition failed, thread collision.", e);
			return null;
		}
	}

	private void setQueueInfoCount(String queue, TaskState oldState, TaskState newState) {

		QueueInfo exists = sfy.get(QueueInfo.class).key(queue).now();
		if (exists == null) {
			return;
		}

		try {

			// atomic counting of tasks in queue position
			if (oldState != null) {
				sfy.command(QueueInfo.class).key(queue).add(oldState.name(), -1).now();
			}

			if (newState != null) {
				sfy.command(QueueInfo.class).key(queue).add(newState.name(), 1).now();
			}

			// total count statistics
			if (TaskState.queued.equals(newState)) {
				sfy.command(QueueInfo.class).key(queue).add("totalTasks", 1).now();
			}

			if (TaskState.finished.equals(newState)) {
				sfy.command(QueueInfo.class).key(queue).add("totalFinished", 1).now();
			}

			if (TaskState.failed.equals(newState)) {
				sfy.command(QueueInfo.class).key(queue).add("totalFailed", 1).now();
			}

			if (TaskState.failed.equals(oldState) &&
				TaskState.running.equals(newState)) {
				sfy.command(QueueInfo.class).key(queue).add("totalRetries", 1).now();
			}
		}
		catch (Exception e) {
			// exception here should not stop working the whole queue
			log.log(Level.SEVERE, "Failed to count tasks!", e);
		}
	}

	/**
	 * Removes job from queue
	 *
	 * @param task to be removed
	 * @return true if task was successfully deleted, false otherwise
	 */
	protected boolean remove(QueueTask task) {

		Assert.notNull(task, "Missing job to be removed!");
		Assert.notNull(task.getId(), "Missing job id: " + task);

		// put job in purge state
		task = transition(task, TaskState.purge);

		if (task != null &&
			TaskState.purge.equals(task.getState())) {
			// transition success - job can be deleted ...
			sfy.delete(task).now();
			return true;
		}

		return false;
	}

	@Override
	public TaskStatistics purge(TaskState state, int taskAge, String queueName) {

		Assert.notNull(state, "Missing job state!");
		Assert.isTrue(state.canTransition(TaskState.purge), "Can't purge tasks in: " + state + " state!");

		List<QueueTask> list = list(state, queueName);

		TaskStatistics.Builder statistics = new TaskStatistics.Builder();

		for (QueueTask item : list) {

			if (item.isLocked() && // task must be locked ... finished, or failed
				item.isOlderThan(taskAge)) {

				if (remove(item)) {

					// calculate statistics ... min, max, average execution duration, average task age ...
					statistics.include(item);
				}
			}
		}

		// join statistic in QueueInfo ... if any
		TaskStatistics output = statistics.build();
		setQueueInfoStatistics(state, queueName, output);

		return output;
	}

	private void setQueueInfoStatistics(final TaskState state, final String queueName, final TaskStatistics output) {

		sfy.transact(5, new Work<QueueInfo>() {
			@Override
			public QueueInfo run() {

				QueueInfo original = sfy.get(QueueInfo.class).key(queueName).now();
				if (original == null) {
					return null;
				}

				original.setStatistics(state, output);
				sfy.update(original).now();
				return original;
			}});
	}
}
