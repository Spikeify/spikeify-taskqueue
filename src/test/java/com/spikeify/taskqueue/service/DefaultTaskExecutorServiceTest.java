package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskResult;
import com.spikeify.taskqueue.TestHelper;
import com.spikeify.taskqueue.TestTask;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskResultState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DefaultTaskExecutorServiceTest {

	private static final String QUEUE = "default";

	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();
		spikeify.truncateSet(QueueTask.class);
	}

	@Test
	public void testContentionOnTask() throws Exception {

		TaskQueueService queueService = new DefaultTaskQueueService(spikeify, 10);
		TaskQueueService spiedQueueService = Mockito.spy(queueService);

		TaskExecutorService service1 = new DefaultTaskExecutorService(spiedQueueService, QUEUE);
		TaskExecutorService service2 = new DefaultTaskExecutorService(spiedQueueService, QUEUE);
		TaskExecutorService service3 = new DefaultTaskExecutorService(spiedQueueService, QUEUE);

		Job job = new TestTask(0);

		QueueTask task = queueService.add(job, QUEUE);
		Mockito.when(spiedQueueService.next(QUEUE)).thenReturn(task); // same job is returned for all services

		// 1st one executes the job
		TaskResult result = service1.execute(null);
		assertEquals(TaskResultState.ok, result.getState());

		// 2nd one collides with job in finished state
		result = service2.execute(null);
		assertNull(result);

		// 3rd one collides with job in finished state
		result = service3.execute(null);
		assertNull(result);
	}
}