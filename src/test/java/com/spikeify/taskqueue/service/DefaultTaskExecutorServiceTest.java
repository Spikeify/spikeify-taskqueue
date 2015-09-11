package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.*;
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
		spikeify.truncateNamespace("test");
	}

	@Test
	public void testContentionOnTask() throws Exception {

		TaskQueueService queueService = new DefaultTaskQueueService(spikeify);
		TaskQueueService spiedQueueService = Mockito.spy(queueService);

		TaskExecutorService service1 = new DefaultTaskExecutorService(spiedQueueService, QUEUE);
		TaskExecutorService service2 = new DefaultTaskExecutorService(spiedQueueService, QUEUE);
		TaskExecutorService service3 = new DefaultTaskExecutorService(spiedQueueService, QUEUE);

		Job job = new TestTask(0);

		queueService.add(job, QUEUE);

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

	@Test
	public void testMaxRetriesOnFailingTask() {

		TaskQueueService queueService = new DefaultTaskQueueService(spikeify);
		TaskQueueService spiedQueueService = Mockito.spy(queueService);

		TaskExecutorService service = new DefaultTaskExecutorService(spiedQueueService, QUEUE);

		Job job = new FailOnlyTask();
		queueService.add(job, QUEUE);

		// 1st one executes the job
		TaskResult result = service.execute(null);
		assertEquals(TaskResultState.failed, result.getState());

		result = service.execute(null);
		assertEquals(TaskResultState.failed, result.getState());

		result = service.execute(null);
		assertEquals(TaskResultState.failed, result.getState());

		// no task should be present 3times retried tasks are locked for execution
		result = service.execute(null);
		assertNull(result);
	}
}