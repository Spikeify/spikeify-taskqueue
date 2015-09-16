package com.spikeify.taskqueue.service;

import com.aerospike.client.AerospikeException;
import com.spikeify.ResultSet;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyService;
import com.spikeify.Work;
import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
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

		// check if ID is uniqe
		sfy.transact(5, new Work<QueueTask>() {
			@Override
			public QueueTask run() {

				QueueTask original = sfy.get(QueueTask.class).key(task.getId()).now();

				if (original != null) { // we have a duplicate ... regenerate job id
					log.warning("Duplicate id of job in queue: " + task.getId() + ", forcing regeneration of id!");
					task.generateId();
					throw new AerospikeException(3); // keep retrying
				}

				sfy.create(task).now();
				return task;
			}
		});

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

		// note: query can return task that are not open anymore ... so choosing random task is ensures that tasks are distributed more or less evenly among workers
		ResultSet<QueueTask> openTasks = sfy.query(QueueTask.class)
											.filter("lockFilter", QueueTask.getLockedFilter(queueName, false))
											.now();

		// Choose random job ... not the first one
		List<QueueTask> list = openTasks.toList();
		if (list.size() == 0) {
			return null;
		}

		// sort by updateTime ... the older task are on top ...
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

			return updated;
		}
		catch (ConcurrentModificationException | AerospikeException e) {
			// job modified by other thread ... transition failed
			log.info("Could not transition job: " + task + " to: " + newState + ", thread collision!");
			return null;
		}
		catch (TaskQueueError e) {

			log.log(Level.SEVERE, "Transition failed, thread collision.", e);
			return null;
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
	 public int purge(TaskState state, int taskAge, String queueName) {

		Assert.notNull(state, "Missing job state!");
		Assert.isTrue(state.canTransition(TaskState.purge), "Can't purge tasks in: " + state + " state!");

		int count = 0;
		List<QueueTask> list = list(state, queueName);
		for (QueueTask item : list) {

			if (item.isLocked() && // task must be locked ... finished, or failed
				item.isOlderThan(taskAge)) {
				if (remove(item)) {
					count++;
				}
			}
		}

		return count;
	}
}
