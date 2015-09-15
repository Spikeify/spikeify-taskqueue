package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.StringUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Each executor executes only one job at most
 * Create multiple executor services in order to run several jobs concurrent
 */
public class DefaultTaskExecutorService implements TaskExecutorService {

	public static final Logger log = Logger.getLogger(DefaultTaskExecutorService.class.getSimpleName());

	/**
	 * times a job is retrieved and tried to be started when given from a queue ... no job is executed
	 */
	private static final int MAX_START_RETRIES = 3;
/*
	*//**
	 * Number of seconds executor waits for interrupt to perform his action
	 *//*
	private static final int MAX_INTERRUPT_WAIT = 5;*/

	private final String queueName;
	private final TaskQueueService queue;

	/**
	 * Job executed by instance of task executor service
	 */
	private static Job currentJob = null;

	public DefaultTaskExecutorService(TaskQueueService queueService, String queueName) {

		Assert.notNull(queueService, "Missing queue service!");

		this.queue = queueService;

		if (StringUtils.isNullOrEmptyTrimmed(queueName)) {
			this.queueName = DefaultTaskQueueService.DEFAULT_QUEUE_NAME;
		}
		else {
			this.queueName = queueName;
		}
	}

	@Override
	public TaskResult execute(TaskContext context) {

		boolean started;
		int retries = 0;

		do {
			// there is a task running ... return null ... execution must wait ...
			if (currentJob != null) {
				return null;
			}

			// 1. get next job to be executed (in running state)
			QueueTask next = queue.next(queueName);

			// no job found ... exit
			if (next == null) {
				break;
			}

			// check if job in running state
			started = TaskState.running.equals(next.getState());

			// was successfully put in running state
			if (started) {
				// get job
				currentJob = next.getJob();

				try {

					// 2. execute job
					TaskResult result = currentJob.execute(context);

					// 3. set job to finished or failed ...
					if (TaskResultState.ok.equals(result.getState())) {
						queue.transition(next, TaskState.finished);
					}
					else {
						log.info("Task resulted in: " + result);
						queue.transition(next, TaskState.failed);
					}

					// 4. end execution
					return result;
				}
				catch (Exception e) {
					log.log(Level.SEVERE, "Failed to execute job: " + currentJob + ", queue id:" + next.getId(), e);

					next = queue.transition(next, TaskState.failed);
					if (next == null || !TaskState.failed.equals(next.getState())) {
						log.log(Level.SEVERE, "Failed to transition queued job to failed state: " + next + "!", e);
					}

					return new TaskResult(TaskResultState.failed);
				}
				finally {
					currentJob = null;
				}
			}
			else {
				// job already in running state (other thread took over ... let's retry)
				retries++; // increase retry counter

				if (retries < MAX_START_RETRIES) {
					try { // wait some time ... before retrying (the more retries the longer we should wait)
						Thread.sleep((long) (10.0D + Math.random() * 10.0D * (double) retries));
					}
					catch (InterruptedException e) {
						log.log(Level.SEVERE, "Thread.sleep() InterruptedException: ", e);
					}
				}
			}
		}
		while (retries < MAX_START_RETRIES);

		// nothing to do ... queue is empty
		return null;
	}

	/**
	 * Will interrupt a running job if any
	 *
	 * @return number of stopped tasks
	 *//*
	@Override
	public void interrupt() {

		// let's interrupt all running tasks ... if any
		if (currentJob != null) {
			currentJob.interrupt(); // signal was send ...

			for (int i = 1; i <= MAX_INTERRUPT_WAIT; i++) {

				// wait ... until job ends
				if (!isRunning()) {
					break;
				}

				try {
					log.info("Waiting for job to interrupt (" + i + "/" + MAX_INTERRUPT_WAIT + ")");
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					log.log(Level.WARNING, "Thread was interrupted!", e);
				}
			}
		}
	}*/

	@Override
	public boolean isRunning() {

		return currentJob != null;
	}
}
