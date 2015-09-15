package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.Work;
import com.spikeify.commands.AcceptFilter;
import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class DefaultTaskQueueManager implements TaskQueueManager {

	public static final Logger log = Logger.getLogger(DefaultTaskExecutorService.class.getSimpleName());

	/**
	 * Number of seconds schedule sleeps when no tasks are available
	 */
	private static final long SLEEP_WAITING_FOR_TASKS = 10;

	/**
	 * Number of seconds waiting to trigger purge action on queue
	 */
	private static final long SLEEP_WAITING_FOR_PURGE = 60;

	/**
	 * Number of seconds waiting for a task to shut down gracefully
	 */
	private static final long SHUTDOWN_TIME = 60;

	private final Spikeify sfy;
	private final TaskQueueService queues;

	/**
	 * Thread pool storage ... single instance for each JVM
	 */
	private static final Map<String, ScheduledExecutorService> threadPool = new HashMap<>();

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
	public void start(String queueName) throws InterruptedException {

		QueueInfo found = find(queueName);
		Assert.notNull(found, "Queue: " + queueName + ", is not registered!");

		// enable queue
		found = save(found, true);

		String name = found.getName();

		// stop thread running if any ...
		stopRunningThreads(name);

		// will start x-threads per queue and monitor them (every 10 seconds)
		ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(found.getSettings().getMaxThreads());

		// queue execution
		TaskContext context = new TaskThreadPoolContext(executorService);

		executorService.scheduleAtFixedRate(new QueueScheduler(queues, name, context),
											0,
											SLEEP_WAITING_FOR_TASKS,
											TimeUnit.SECONDS);

		// failed task purging
		executorService.scheduleAtFixedRate(new QueuePurger(queues, name, TaskState.failed, found.getSettings().getPurgeFailedAfterMinutes()),
											SLEEP_WAITING_FOR_PURGE,
											SLEEP_WAITING_FOR_PURGE,
											TimeUnit.SECONDS);

		// finished task purging
		executorService.scheduleAtFixedRate(new QueuePurger(queues, name, TaskState.finished, found.getSettings().getPurgeSuccessfulAfterMinutes()),
											SLEEP_WAITING_FOR_PURGE,
											SLEEP_WAITING_FOR_PURGE,
											TimeUnit.SECONDS);

		// store execution into thread pool by queue name
		threadPool.put(name, executorService);
	}

	@Override
	public void stop(String queueName) throws InterruptedException {

		QueueInfo found = find(queueName);
		Assert.notNull(found, "Queue: " + queueName + " is not registered!");

		// disabled queue
		found = save(found, false);

		stopRunningThreads(found.getName());
	}

	private QueueInfo save(QueueInfo found, boolean enabled) {

		return sfy.transact(5, new Work<QueueInfo>() {
			@Override
			public QueueInfo run() {

				QueueInfo original = sfy.get(QueueInfo.class).key(found.getName()).now();
				original.setEnabled(enabled);

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
