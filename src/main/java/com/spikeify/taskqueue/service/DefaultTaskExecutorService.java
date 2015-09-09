package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultTaskExecutorService implements TaskExecutorService {

	public static final Logger log = Logger.getLogger(DefaultTaskExecutorService.class.getSimpleName());

	// times a job is retrieved and tried to be started when given from a queue ... no job is executed
	private static final int MAX_START_RETRIES = 3;

	private final String queueName;
	private final TaskQueueService queue;

	public DefaultTaskExecutorService(TaskQueueService queueService, String queueName) {

		this.queue = queueService;
		this.queueName = queueName;
	}

	@Override
	public TaskResult execute(TaskContext context) {

		// 1. get next job to be executed
		// 2. set job state to running
		// 3. execute job
		// 4. set job to finished or failed ...

		boolean started;
		int retries = 0;

		do {
			QueueTask next = queue.next(queueName);

			// no job found ... exit
			if (next == null) {
				break;
			}

			// put job in running state
			started = queue.transition(next, TaskState.running);

			// was successfully put in running state
			if (started)
			{
				// get job
				Job task = next.getJob();

				try {

					// execute job
					TaskResult result = task.execute(context);

					// put in success or failed state
					if (TaskResultState.ok.equals(result.getState())) {
						queue.transition(next, TaskState.finished);
					}
					else {
						queue.transition(next, TaskState.failed);
					}

					// end execution
					return result;
				}
				catch (Exception e) {
					log.log(Level.SEVERE, "Failed to execute job: " + task + ", queue id:" + next.getId(), e);

					boolean hasFailed = queue.transition(next, TaskState.failed);
					if (!hasFailed) {
						log.log(Level.SEVERE, "Failed to transition queued job to failed state: " + next + "!", e);
					}

					return new TaskResult(TaskResultState.failed);
				}
			}
			else {
				retries ++; // increase retry counter

				if (retries < MAX_START_RETRIES) {
					try { // wait some time ... before retrying (the more retries the longer we should wait)
						Thread.sleep((long)(10.0D + Math.random() * 10.0D * (double)retries));
					} catch (InterruptedException e) {
						log.log(Level.SEVERE, "Thread.sleep() InterruptedException: ", e);
					}
				}
			}
		}
		while (!started && retries < MAX_START_RETRIES);

		// nothing to do ... queue is empty
		return null;
	}

	@Override
	public int purge(TaskState state) {

		Assert.notNull(state, "Missing job state!");
		Assert.isTrue(state.canTransition(TaskState.purge), "Can't purge tasks in: " + state + " state!");

		int count = 0;
		List<QueueTask> list = queue.list(state, queueName);
		for (QueueTask item : list) {

			if (queue.remove(item)) {
				count++;
			}
		}

		return count;
	}
}
