package com.spikeify.taskqueue.service;

import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.TaskContext;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskResultState;
import com.spikeify.taskqueue.entities.TaskState;
import com.spikeify.taskqueue.utils.Assert;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultTaskExecutorService implements TaskExecutorService {

	public static final Logger log = Logger.getLogger(DefaultTaskExecutorService.class.getSimpleName());

	private final String queueName;
	private final TaskQueueService queue;

	public DefaultTaskExecutorService(TaskQueueService queueService, String queueName) {

		this.queue = queueService;
		this.queueName = queueName;
	}

	@Override
	public TaskResult execute(TaskContext context) {

		// 1. get next task to be executed
		// 2. set task state to running
		// 3. execute task
		// 4. set task to finished or failed ...

		QueueTask next = queue.next(queueName);

		if (next == null) {
			// nothing to do ... queue is empty
			return null;
		}

		if (queue.transition(next, TaskState.running)) {

			Task task = next.getTask();

			try {
				TaskResult result = task.execute(context);

				if (TaskResultState.ok.equals(result.getState())) {
					queue.transition(next, TaskState.finished);
				}
				else {
					queue.transition(next, TaskState.failed);
				}

				return result;
			}
			catch (Exception e) {
				log.log(Level.SEVERE, "Failed to execute task: " + task + ", queue id:" + next.getId(), e);

				boolean hasFailed = queue.transition(next, TaskState.failed);
				if (!hasFailed) {
					log.log(Level.SEVERE, "Failed to transition queued task to failed state: " + next + "!", e);
				}
			}
		}

		return null;
	}

	@Override
	public void purge(TaskState state) {

		Assert.notNull(state, "Missing task state!");


	}
}
