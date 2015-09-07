package com.spikeify.taskqueue.service;

import com.aerospike.client.AerospikeException;
import com.spikeify.ResultSet;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyService;
import com.spikeify.Work;
import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.StringUtils;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.logging.Logger;

public class DefaultTaskQueueService implements TaskQueueService {

	private static final Logger log = Logger.getLogger(DefaultTaskQueueService.class.getSimpleName());

	private static final String DEFAULT_QUEUE_NAME = "default";

	private final Spikeify sfy;

	public DefaultTaskQueueService(Spikeify spikeify) {

		sfy = spikeify;
		// create indexes if not already present ...
		SpikeifyService.register(QueueTask.class);
	}

	@Override
	public QueueTask add(Task job, String queueName) {

		if (StringUtils.isNullOrEmptyTrimmed(queueName)) {
			queueName = DEFAULT_QUEUE_NAME;
		}

		QueueTask task = new QueueTask(job, queueName);

		// check if ID is uniqe
		sfy.transact(5, new Work<QueueTask>() {
			@Override
			public QueueTask run() {

				QueueTask original = sfy.get(QueueTask.class).key(task.getId()).now();

				if (original != null) { // we have a duplicate ... regenerate task id
					log.warning("Duplicate id of task in queue: " + task.getId() + ", forcing regeneration of id!");
					task.generateId();
					throw new AerospikeException(3); // keep retrying
				}

				sfy.create(task).now();
				return task;
			}
		});

		// create id ... add task ...
		return task;
	}

	/**
	 * Returns next open task ... might be that in the mean time some other thread will lock this task ... so multiple calls to this method are possible
	 *
	 * @param queueName name of queue
	 * @return found task or null if none found
	 */
	@Override
	public QueueTask next(String queueName) {

		if (StringUtils.isNullOrEmptyTrimmed(queueName)) {
			queueName = DEFAULT_QUEUE_NAME;
		}

		ResultSet<QueueTask> openTasks = sfy.query(QueueTask.class)
											.filter("lockedFilter", QueueTask.getLockedFilter(queueName, false))
											.now();

		Iterator<QueueTask> iterator = openTasks.iterator();
		if (iterator.hasNext()) {
			return iterator.next();
		}

		return null;
	}

	@Override
	public boolean transition(QueueTask task, TaskState newState) {

		Assert.notNull(task, "Missing task!");
		Assert.notNull(newState, "Missing state!");

		try {

			QueueTask updated = sfy.transact(5, new Work<QueueTask>() {
				@Override
				public QueueTask run() {

					QueueTask original = sfy.get(QueueTask.class).key(task.getId()).now();

					TaskState old = original.getState();
					original.setState(newState);

					if (old.equals(newState)) { // transition failed ... retry
						throw new AerospikeException(3); // keep retrying
					}

					sfy.update(original).now();

					return original;
				}
			});

			return updated.getState().equals(newState);
		}
		catch (ConcurrentModificationException | AerospikeException e) {
			// task modified by other thread ... transition failed
			log.info("Could not transition task to: " + newState + ", thread collision!");
			return false;
		}
	}

	/**
	 * Removes task from queue
	 *
	 * @param task to be removed
	 */
	@Override
	public void remove(QueueTask task) {

		Assert.notNull(task, "Missing task to be removed!");
		Assert.notNull(task.getId(), "Missing task id: " + task);

		sfy.delete(task).now();
	}
}
