package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.Work;
import com.spikeify.commands.AcceptFilter;
import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.entities.QueueInfoUpdater;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.QueueSettings;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Should be used as a singleton ...
 * one instance per JVM
 */
public class DefaultTaskQueueManager implements TaskQueueManager {

	public static final Logger log = Logger.getLogger(DefaultTaskExecutorService.class.getSimpleName());

	/**
	 * Number of seconds to wait before starting up scheduling
	 */
	static final long SLEEP_WAITING_FOR_START = 0;

	/**
	 * Number of seconds schedule sleeps when no tasks are available
	 */
	static final long SLEEP_WAITING_FOR_TASKS = 10;

	/**
	 * Number of seconds waiting to trigger purge action on queue
	 */
	static final long SLEEP_WAITING_FOR_PURGE = 60;

	/**
	 * Number of seconds waiting for a task to shut down gracefully
	 */
	static final long SHUTDOWN_TIME = 60;


	private final Spikeify sfy;
	private final TaskQueueService queues;

	/**
	 * Thread pool storage ... single instance for each task manager
	 */
	private final Map<String, ScheduledExecutorService> threadPool = new HashMap<>();

	public DefaultTaskQueueManager(Spikeify spikeify,
								   TaskQueueService queueService) {

		Assert.notNull(spikeify, "Missing spikeify!");
		sfy = spikeify;

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

				if (original != null) { // already registered ... just return original
					return original;
				}

				// create default queue info ...
				QueueInfo newQueue = new QueueInfo(name);

				sfy.create(newQueue).now();
				return newQueue;
			}
		});

		log.info("Queue: " + name + ", registered!");
		return queue;
	}

	@Override
	public List<QueueInfo> list(Boolean active) {

		return sfy.scanAll(QueueInfo.class).filter(new AcceptFilter<QueueInfo>() {
			@Override
			public boolean accept(QueueInfo queueInfo) {

				return (active == null || active == queueInfo.isEnabled());
			}
		}).now();
	}

	@Override
	public void unregister(String queueName) {

		QueueInfo found = find(queueName);

		try {

			stop(queueName);

			// clean all tasks from queue ... none should be running anymore
			queues.purge(TaskState.queued, 0, queueName);
			queues.purge(TaskState.failed, 0, queueName);
			queues.purge(TaskState.finished, 0, queueName);
			queues.purge(TaskState.interrupted, 0, queueName);

			if (found != null) {
				log.info("Queue: " + queueName + ", unregistered!");
				sfy.delete(found).now();
			}

		}
		catch (InterruptedException e) {
			log.log(Level.SEVERE, "Failed to stop queue: " + queueName + ", can't unregister!", e);
		}
	}

	@Override
	public void start(String... queueNames) throws InterruptedException {

		List<QueueInfo> found = getQueues(queueNames);

		for (QueueInfo queue : found) {

			String name = queue.getName();
			QueueSettings settings = queue.getSettings();

			startQueue(name, true);

			// stop thread running if any ...
			stopRunningThreads(name);

			// will start x-threads per queue and monitor them (every 10 seconds)
			ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(settings.getMaxThreads());

			// queue execution
			TaskContext context = new TaskThreadPoolContext(executorService);

			executorService.scheduleAtFixedRate(new QueueScheduler(queues, name, context),
												SLEEP_WAITING_FOR_START,
												SLEEP_WAITING_FOR_TASKS,
												TimeUnit.SECONDS);

			// failed task purging
			executorService.scheduleAtFixedRate(new QueuePurger(queues, name, TaskState.failed, settings.getPurgeFailedAfterMinutes()),
												SLEEP_WAITING_FOR_PURGE,
												SLEEP_WAITING_FOR_PURGE,
												TimeUnit.SECONDS);

			// finished task purging
			executorService.scheduleAtFixedRate(new QueuePurger(queues, name, TaskState.finished, settings.getPurgeSuccessfulAfterMinutes()),
												SLEEP_WAITING_FOR_PURGE,
												SLEEP_WAITING_FOR_PURGE,
												TimeUnit.SECONDS);

			// store execution into thread pool by queue name
			threadPool.put(name, executorService);
			log.info("Started queue: " + name);
		}
	}

	@Override
	public void stop(String... queueNames) throws InterruptedException {

		List<QueueInfo> found = getQueues(queueNames);

		for (QueueInfo queue : found) {

			String name = queue.getName();
			startQueue(name, false);

			stopRunningThreads(name);
			log.info("Stopped queue: " + name);
		}
	}

	@Override
	public QueueInfo enable(String queueName) {

		return save(queueName, new QueueInfoUpdater() {
			@Override
			public void update(QueueInfo info) {

				info.setEnabled(true);
			}
		});
	}

	@Override
	public QueueInfo disable(String queueName) {

		return save(queueName, new QueueInfoUpdater() {
			@Override
			public void update(QueueInfo info) {

				info.setEnabled(false);
				// set start false will trigger queue stop when check is called
				info.setStarted(false);
			}
		});
	}

	@Override
	public void check() throws InterruptedException {

		List<QueueInfo> queues = list(true); // only enabled queues are "checked"

		// check if all queues are started on this JVM
		for (QueueInfo info : queues) {

			// get latest from database
			QueueInfo original = sfy.get(QueueInfo.class).key(info.getName()).now();

			boolean isStarted = original.isStarted();
			boolean isInPool = threadPool.containsKey(original.getName());

			// queue should be started but is not
			if (isStarted && !isInPool) {
				start(original.getName());
				continue;
			}

			// queue should be stopped ..
			if (!isStarted && isInPool) {
				stop(original.getName());
			}
		}
	}

	private List<QueueInfo> getQueues(String... queueNames) {

		if (queueNames == null || queueNames.length == 0) {
			return list(true);
		}
		else {
			ArrayList<QueueInfo> list = new ArrayList<>();

			for (String name : queueNames) {

				QueueInfo queue = find(name);
				Assert.notNull(queue, "Queue: " + name + ", is not registered!");
				Assert.isTrue(queue.isEnabled(), "Queue: " + name + " is not enabled!");

				list.add(queue);
			}

			return list;
		}
	}

	private QueueInfo startQueue(final String queueName, boolean start) {

		return save(queueName, new QueueInfoUpdater() {
			@Override
			public void update(QueueInfo info) {

				info.setStarted(start);
			}
		});
	}

	private QueueInfo save(String queueName, QueueInfoUpdater updater) {

		return sfy.transact(5, new Work<QueueInfo>() {
			@Override
			public QueueInfo run() {

				QueueInfo original = sfy.get(QueueInfo.class).key(queueName).now();
				updater.update(original);

				sfy.update(original).now();
				return original;
			}
		});
	}

	protected void stopRunningThreads(String queueName) throws InterruptedException {

		ScheduledExecutorService running = threadPool.get(queueName);

		if (running != null) {
			// interrupt all running schedules if any
			running.shutdown();

			if (!running.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
				log.warning("Executor did not terminate in the specified time.");

				List<Runnable> droppedTasks = running.shutdownNow();
				log.warning("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
			}
		}

		threadPool.remove(queueName);
	}

	protected QueueInfo find(String queueName) {

		Assert.notNullOrEmpty(queueName, "Missing queue name!");
		final String name = queueName.trim();

		return sfy.get(QueueInfo.class).key(name).now();
	}
}
