package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;
import com.spikeify.taskqueue.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each executor executes only one job at most
 * Create multiple executor services in order to run several jobs concurrent
 */
public class DefaultTaskExecutorService implements TaskExecutorService {

	public static final Logger log = LoggerFactory.getLogger(DefaultTaskExecutorService.class);

	/**
	 * times a job is retrieved and tried to be started when given from a queue ... no job is executed
	 */
	private static final int MAX_START_RETRIES = 3;

	private final String queueName;
	private final TaskQueueService queue;

	// current running job
	private Job currentJob;

	public DefaultTaskExecutorService(TaskQueueService queueService,
									  String queueName) {

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
			if (currentJob != null) {
				return null; // already executing ... wait
			}

			// 1. get next job to be executed (in running state)
			QueueTask next = queue.next(queueName);

			// no job found ... exit
			if (next == null) {
				break;
			}

			// check if job in running state
			// (this might be the case as some other queue thread grabbed the task while it was in "transition")
			started = TaskState.running.equals(next.getState());

			// was successfully put in running state
			if (started) {

				try {
					// get job
					currentJob = next.getJob();

					// 2. execute job
					TaskResult result = currentJob.execute(context);

					// 3. set job to finished, interrupted or failed ...
					switch (result.getState()) {
						case ok:
							queue.transition(next, TaskState.finished);
							break;

						case interrupted:
							queue.transition(next, TaskState.interrupted);
							break;

						default:
						case failed:
							queue.transition(next, TaskState.failed);
							break;
					}

					log.debug("Task resulted in: " + result);

					// 4. end execution
					return result;
				}
				catch (Exception e) {
					log.error("Failed to execute job: " + currentJob + ", queue id:" + next.getId(), e);

					next = queue.transition(next, TaskState.failed);
					if (next == null || !TaskState.failed.equals(next.getState())) {
						log.error("Failed to transition queued job to failed state: " + next + "!", e);
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
						log.error("Thread.sleep() InterruptedException: ", e);
					}
				}
			}
		}
		while (retries < MAX_START_RETRIES);

		// nothing to do ... queue is empty
		return null;
	}

	@Override
	public boolean isRunning() {

		return currentJob != null;
	}

	@Override
	public void reset() {

		currentJob = null;
	}
}
