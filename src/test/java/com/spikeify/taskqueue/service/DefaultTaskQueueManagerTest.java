package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.*;
import com.spikeify.taskqueue.entities.QueueInfo;
import com.spikeify.taskqueue.entities.QueueSettings;
import com.spikeify.taskqueue.entities.QueueTask;
import com.spikeify.taskqueue.entities.TaskState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.spikeify.taskqueue.service.DefaultTaskQueueManager.SLEEP_WAITING_FOR_TASKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultTaskQueueManagerTest {

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

	@After
	public void tearDown() {

//		spikeify.truncateNamespace("test");
	}

	@Test
	public void testRegisterEnableDisableUnregister() throws Exception {

		manager.register("test");
		manager.register("test2");
		manager.register("test3");

		List<QueueInfo> list = manager.list(true);
		assertEquals(3, list.size());

		// nothing to do
		manager.enable("test");

		list = manager.list(true);
		assertEquals(3, list.size());

		manager.disable("test2");

		list = manager.list(true);
		assertEquals(2, list.size());

		list = manager.list(false);
		assertEquals(1, list.size());
		QueueInfo queue = list.get(0);

		assertFalse(queue.isEnabled());
		assertFalse(queue.isStarted());

		// queues are started if not started
		manager.start();
		list = manager.list(true);

		assertEquals(2, list.size());
		for (QueueInfo info: list) {
			assertTrue(info.isStarted());
			assertTrue(info.getName().equals("test") || info.getName().equals("test3"));
		}

		// disable one queue ..
		manager.disable("test");

		// queue should be stop
		manager.check();

		manager.start();
		list = manager.list(true);

		assertEquals(1, list.size());

		QueueInfo info = list.get(0);
		assertTrue(info.isStarted());
		assertTrue(info.getName().equals("test3"));
	}


	@Test
	public void testStart() throws Exception {

		String QUEUE = "simple";
		manager.register(QUEUE); // create queue

		for (int i = 0; i < 3; i++) {
			// add some long running tasks
			queues.add(new TestTask(i), QUEUE);
		}

		manager.start(QUEUE);

		// wait for task to execute (5s most)
		Thread.sleep(2000);


		queues.add(new TestTask(4), QUEUE); // add one more tasks

		List<QueueTask> list = queues.list(TaskState.finished, QUEUE);
		assertEquals(3, list.size());

		list = queues.list(TaskState.queued, QUEUE);
		assertEquals(1, list.size());

		// wait so task can finish (10s)
		Thread.sleep(SLEEP_WAITING_FOR_TASKS * 1000);


		list = queues.list(TaskState.finished, QUEUE);
		assertEquals(4, list.size());
	}

	@Test
	public void testStop() throws Exception {

		// start and then abruptly stop
		String QUEUE = "testStop";
		manager.register(QUEUE); // create queue

		for (int i = 0; i < 20; i++) { // add few long lasting tasks
			// add some long running tasks
			queues.add(new LongRunningTask(1000), QUEUE);
		}

		manager.start(QUEUE);

		Thread.sleep(4500); // let at least 4 tasks finish

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

	@Test
	public void testMultipleMachinesRunningQueues() throws InterruptedException {

		String QUEUE = "testMultipleMachinesRunningQueues";
		manager.register(QUEUE);

		for (int i = 0; i < 100; i++) { // add a lot 1s tasks
			// add some long running tasks
			queues.add(new LongRunningTask(1000), QUEUE);
		}

		int MACHINES = 3;

		DefaultTaskQueueManager[] manager = new DefaultTaskQueueManager[MACHINES];
		for (int i = 0; i < MACHINES; i++) {
			manager[i] = new DefaultTaskQueueManager(spikeify, queues);

			if (i == 0) {
				manager[i].start(QUEUE);
			}
			else {
				manager[i].check();
			}
		}

		// wait so task can finish (10s)
		Thread.sleep(SLEEP_WAITING_FOR_TASKS * 1000);

		for (int i = MACHINES - 1; i >= 0; i--) {
			if (i == 0) {
				manager[i].stop(QUEUE);
			}
			else {
				manager[i].check();
			}
		}

		// check how many tasks have been finished
		List<QueueTask> list = queues.list(TaskState.finished, QUEUE);
		assertTrue("Only: " + list.size() + " tasks finished, expected: " + ((MACHINES * SLEEP_WAITING_FOR_TASKS) - (MACHINES * 3)), list.size() >= (MACHINES * SLEEP_WAITING_FOR_TASKS) - (MACHINES * 3)); // at least so many task should have been finished
	}

	@Test
	public void killTimedOutTask() throws InterruptedException {

		String QUEUE = "killTimedOutTask";
		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		TaskExecutorService executor = new DefaultTaskExecutorService(service, QUEUE);
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE);

		service.add(new TimeoutTask(), QUEUE);

		QueueInfo info = manager.info(QUEUE);
		QueueSettings settings = info.getSettings();
		settings.setTaskTimeoutSeconds(10);

		manager.set(QUEUE, settings); // 10 seconds for tasks to time out

		manager.start(QUEUE);

		Thread.sleep(20 * 1000); // wait 20 seconds ... task should be put in failed state at least once

		// task should be in running state ...
		List<QueueTask> list = queues.list(TaskState.running, QUEUE);
		assertEquals(1, list.size());

		// let's purge
		QueuePurger purger = new QueuePurger(queues, QUEUE, settings);
		purger.run();

		// task should be moved into failed state
		list = queues.list(TaskState.running, QUEUE);
		assertEquals(0, list.size());

		list = queues.list(TaskState.failed, QUEUE);
		assertEquals(1, list.size());
	}
}