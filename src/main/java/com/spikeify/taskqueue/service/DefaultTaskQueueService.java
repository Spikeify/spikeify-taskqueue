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
import com.spikeify.taskqueue.utils.StringUtils;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultTaskQueueService implements TaskQueueService {

	private static final Logger log = Logger.getLogger(DefaultTaskQueueService.class.getSimpleName());

	private static final String DEFAULT_QUEUE_NAME = "default";
	private static final int CHOOSE_NEXT_TASK_RETRIES = 10;

	private final Spikeify sfy;
	private final int workers; // number of workers

	public DefaultTaskQueueService(Spikeify spikeify) {

		this(spikeify, 1);
	}

	public DefaultTaskQueueService(Spikeify spikeify, int numberOfWorkers) {

		sfy = spikeify;
		// create indexes if not already present ...
		SpikeifyService.register(QueueTask.class);

		Assert.isTrue(numberOfWorkers > 0, "Number of workers must be > 0!");
		workers = numberOfWorkers;
	}

	@Override
	public QueueTask add(Job job, String queueName) {

		if (StringUtils.isNullOrEmptyTrimmed(queueName)) {
			queueName = DEFAULT_QUEUE_NAME;
		}

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
	 * Returns next open job ... might be that in the mean time some other thread will lock this job ... so multiple calls to this method are possible
	 *
	 * @param queueName name of queue
	 * @return found job or null if none found
	 */
	@Override
	public QueueTask next(String queueName) {

		if (StringUtils.isNullOrEmptyTrimmed(queueName)) {
			queueName = DEFAULT_QUEUE_NAME;
		}

		// note: query can return task that are not open anymore ... so choosing random task is ensures that tasks are distributed more or less evenly among workers
		ResultSet<QueueTask> openTasks = sfy.query(QueueTask.class)
											.filter("lockFilter", QueueTask.getLockedFilter(queueName, false))
											.now();

		// Choose random job ... not the first one
		List<QueueTask> list = openTasks.toList();

		if (list.size() == 0) {
			return null;
		}

		int size = Math.min(10, list.size()); // 10 or less random from list
		QueueTask proposed = null;

		// try to find open task ...
		for (int i = 1; i <= CHOOSE_NEXT_TASK_RETRIES; i++) {
			Random rand = new Random();
			int idx = rand.nextInt(size);

			proposed = list.get(idx);
			QueueTask task = sfy.get(QueueTask.class).key(proposed.getId()).now();

			// check if task has not been altered by other queue
			if (!task.isLocked()) {
				return task;
			}

			size = Math.min(10 * i, list.size()); // make random choice wider
		}

		// last resort ... (don't return null as null is the signal that there are no new tasks)
		return proposed;
	}

	@Override
	public List<QueueTask> list(TaskState state, String queueName) {

		Assert.notNull(state, "Missing job state!");

		if (StringUtils.isNullOrEmptyTrimmed(queueName)) {
			queueName = DEFAULT_QUEUE_NAME;
		}

		ResultSet<QueueTask> query = sfy.query(QueueTask.class)
										.filter("stateFilter", QueueTask.getStateFilter(queueName, state))
										.now();
		return query.toList();
	}

	@Override
	public boolean transition(QueueTask task, TaskState newState) {

		Assert.notNull(task, "Missing job!");
		Assert.notNull(newState, "Missing state!");

		try {

			QueueTask updated = sfy.transact(1, new Work<QueueTask>() {
				@Override
				public QueueTask run() {

					// get latest version
					QueueTask original = sfy.get(QueueTask.class).key(task.getId()).now();

					// will throw exception in case transition is not possible (we don't have the latest version from database)
					original.setState(newState);

					// update state ...
					sfy.update(original).now();

					return original;
				}
			});

			return updated.getState().equals(newState);
		}
		catch (ConcurrentModificationException | AerospikeException e) {
			// job modified by other thread ... transition failed
			log.info("Could not transition job: " + task + " to: " + newState + ", thread collision!");
			return false;
		}
		catch (TaskQueueError e) {

			log.log(Level.SEVERE, "Transition failed, thread collision.", e);
			return false;
		}
	}

	/**
	 * Removes job from queue
	 *
	 * @param task to be removed
	 */
	@Override
	public boolean remove(QueueTask task) {

		Assert.notNull(task, "Missing job to be removed!");
		Assert.notNull(task.getId(), "Missing job id: " + task);

		// put job in purge state
		if (transition(task, TaskState.purge)) {
			// transition success - job can be deleted ...
			sfy.delete(task).now();
			return true;
		}

		return false;
	}
}
