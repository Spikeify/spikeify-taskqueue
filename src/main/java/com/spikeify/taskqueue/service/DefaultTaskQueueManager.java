package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.Work;
import com.spikeify.commands.AcceptFilter;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;

import java.util.List;
import java.util.logging.Logger;

public class DefaultTaskQueueManager implements TaskQueueManager {

	public static final Logger log = Logger.getLogger(DefaultTaskExecutorService.class.getSimpleName());

	private final Spikeify sfy;

	//private final TaskQueueService queue;
	private final TaskExecutorService executor;
	private final TaskQueueService queues;

	public DefaultTaskQueueManager(Spikeify spikeify,
								   TaskExecutorService executorService,
								   TaskQueueService queueService) {

		Assert.notNull(spikeify, "Missing spikeify!");
		sfy = spikeify;

		executor = executorService;
		queues = queueService;
	}

	@Override
	public QueueInfo register(String queueName) {

		Assert.notNullOrEmpty(queueName, "Missing queue name!");
		final String name = queueName.trim();

		// check if ID is uniqe
		QueueInfo queue = sfy.transact(5, new Work<QueueInfo>() {
			@Override
			public QueueInfo run() {

				QueueInfo original = sfy.get(QueueInfo.class).key(name).now();

				if (original != null) { // we have a duplicate ... regenerate job id
					log.warning("Queue already registered: " + name + "!");
					return original;
				}

				// create default queue info ...
				QueueInfo newQueue = new QueueInfo(name);
				newQueue.setEnabled(false);

				sfy.create(newQueue).now();
				return newQueue;
			}
		});

		return queue;
	}

	@Override
	public List<QueueInfo> list(boolean active) {

		return sfy.scanAll(QueueInfo.class).filter(new AcceptFilter<QueueInfo>() {
			@Override
			public boolean accept(QueueInfo queueInfo) {

				return active == queueInfo.isEnabled();
			}
		}).now();
	}

	@Override
	public void unregister(String queueName) {

		QueueInfo found = find(queueName);

		if (found != null) {
			log.info("Queue: " + queueName + ", unregistered!");
			sfy.delete(found).now();
		}
	}

	@Override
	public void start(String queueName) {

		QueueInfo found = find(queueName);
		Assert.notNull(found, "Queue: " + queueName + " is not registered!");

		found.setEnabled(true);
	}

	@Override
	public void stop(String queueName) {

		QueueInfo found = find(queueName);
		Assert.notNull(found, "Queue: " + queueName + " is not registered!");

		// clean up
		run();

		// disable
		found.setEnabled(false);
	}

	/**
	 * Must be called on regular bases ... for instance every minute
	 */
	@Override
	public void run() {

		List<QueueInfo> list = list(true);

		for (QueueInfo queue : list) {

			queues.purge(TaskState.finished, queue.getSettings().getPurgeSuccessfulAfterMinutes(), queue.getName());

			// purge failed tasks only if allowed
			int failedAge = queue.getSettings().getPurgeFailedAfterMinutes();
			if (failedAge > 0) {
				queues.purge(TaskState.failed, failedAge, queue.getName());
			}
		}
	}

	protected QueueInfo find(String queueName) {

		Assert.notNullOrEmpty(queueName, "Missing queue name!");
		final String name = queueName.trim();

		return sfy.get(QueueInfo.class).key(name).now();
	}
}
