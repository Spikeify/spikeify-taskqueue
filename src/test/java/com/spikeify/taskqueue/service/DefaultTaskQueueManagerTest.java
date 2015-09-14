package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.TestHelper;
import com.spikeify.taskqueue.TestTask;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultTaskQueueManagerTest {

	private static final String QUEUE = "simple";
	private Spikeify spikeify;

	private TaskQueueManager manager;
	private TaskQueueService queues;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();

		spikeify.truncateNamespace("test");

		queues = new DefaultTaskQueueService(spikeify);
		manager = new DefaultTaskQueueManager(spikeify, queues);
	}

	@Test
	public void testRegister() throws Exception {

	}

	@Test
	public void testList() throws Exception {

	}

	@Test
	public void testUnregister() throws Exception {

	}

	@Test
	public void testStart() throws Exception {

		manager.register(QUEUE); // create queue

		for (int i = 0; i < 3; i++) {
			// add some long running tasks
			queues.add(new TestTask(i), QUEUE);
		}

		manager.start(QUEUE);

		//
		for (int i = 0; i < 5; i++) {
			// wait until tasks are finished
			Thread.sleep(1000);
		}

		queues.add(new TestTask(4), QUEUE); // add one more tasks

		//
		for (int i = 0; i < 10; i++) {
			// wait until tasks are finished
			Thread.sleep(1000);
		}

		List<QueueTask> list = queues.list(TaskState.finished, QUEUE);
		assertEquals(4, list.size());
	}

	@Test
	public void testStop() throws Exception {

	}

	@Test
	public void testRun() throws Exception {

	}
}