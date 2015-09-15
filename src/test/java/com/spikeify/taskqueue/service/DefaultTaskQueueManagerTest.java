package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.LongRunningTask;
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

		// wait for task to execute (5s most)
		Thread.sleep(5000);


		queues.add(new TestTask(4), QUEUE); // add one more tasks

		List<QueueTask> list = queues.list(TaskState.finished, QUEUE);
		assertEquals(3, list.size());

		list = queues.list(TaskState.queued, QUEUE);
		assertEquals(1, list.size());

		// wait so task can finish (10s)
		Thread.sleep(10000);


		list = queues.list(TaskState.finished, QUEUE);
		assertEquals(4, list.size());
	}

	@Test
	public void testStop() throws Exception {

		// start and then abruptly stop

		manager.register(QUEUE); // create queue

		for (int i = 0; i < 20; i++) { // add few long lasting tasks
			// add some long running tasks
			queues.add(new LongRunningTask(1000), QUEUE);
		}

		manager.start(QUEUE);

		Thread.sleep(5000); // let at least 4 tasks finish

		// let's stop
		manager.stop(QUEUE);

		// 15 are left in the queue
		List<QueueTask> list = queues.list(TaskState.queued, QUEUE);
		assertEquals(15, list.size());

		// 1 was interrupted
		list = queues.list(TaskState.interrupted, QUEUE);
		assertEquals(1, list.size());

		// 4 manage to finish
		list = queues.list(TaskState.finished, QUEUE);
		assertEquals(4, list.size());

		// none should fail
		list = queues.list(TaskState.failed, QUEUE);
		assertEquals(0, list.size());
	}
}