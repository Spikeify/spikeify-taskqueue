package com.spikeify.taskqueue.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.spikeify.Spikeify;
import com.spikeify.Work;
import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.utils.Assert;

import java.util.logging.Logger;

public class DefaultTaskQueueService implements TaskQueueService {

	private static final Logger log = Logger.getLogger(DefaultTaskQueueService.class.getSimpleName());

	private final Spikeify sfy;

	public DefaultTaskQueueService(Spikeify spikeify, AerospikeClient client, String namespace) {

		sfy = spikeify;
		/*client.createIndex(new Policy(),
						   namespace,
												tableName,
												indexName,
												fieldName,
												IndexType.NUMERIC);*/
	}

	@Override
	public QueueTask add(Task job) {


		QueueTask task = new QueueTask(job);

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

				sfy.create(task);
				return task;
			}
		});

		// create id ... add task ...
		return task;
	}

	@Override
	public QueueTask next() {

	//	sfy.query(QueueTask.class).setFilters(Fi)

		// get next task to be executed ...
		return null;
	}

	@Override
	public void remove(QueueTask task) {

		Assert.notNull(task, "Missing task to be removed!");
		Assert.notNull(task.getId(), "Missing task id: " + task);

		sfy.delete(task);
	}
}
