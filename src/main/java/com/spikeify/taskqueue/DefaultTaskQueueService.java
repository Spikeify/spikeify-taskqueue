package com.spikeify.taskqueue;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.entities.QueueTask;

public class DefaultTaskQueueService implements TaskQueueService {

	private final Spikeify sfy;

	public DefaultTaskQueueService(Spikeify spikeify) {

		sfy = spikeify;
	}

	@Override
	public QueueTask add(Task job) {

		return add(job, TaskPriority.normal);
	}

	@Override
	public QueueTask add(Task job, TaskPriority priority) {

		QueueTask task = new QueueTask(job, priority);
		// create id ... add task ...
		return task;
	}

	@Override
	public QueueTask next() {

		// get next task to be executed ... lock task for other workers ...
		return null;
	}

	@Override
	public TaskResult execute(TaskContext context) {
/*
TODO: something like this ...
		QueueTask queueTask = next();
		lock(queueTask);

		TaskResult result = null;

		try {
			Task job = queueTask.getTask();
			result = job.execute(context);
		}
		catch (Exception e) {
			retry(queueTask);
		}
		finally {
			unlock(queueTask);
		}
		return result;
		*/

		return null;
	}
}
