package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.LongRunningTask;
import com.spikeify.taskqueue.TestHelper;
import com.spikeify.taskqueue.TestTask;
import com.spikeify.taskqueue.TimeoutTask;
import com.spikeify.taskqueue.entities.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.spikeify.taskqueue.service.DefaultTaskQueueManager.log;
import static org.junit.Assert.*;

public class DefaultTaskQueueManagerTest {

	private static final int SLEEP_WAITING_FOR_TASKS = 10;

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

	//	spikeify.truncateNamespace("test");
	}

	@Test
	public void testRegisterEnableDisableUnregister() throws Exception {

		manager.register("test", false);
		manager.register("test2", false);
		manager.register("test3", false);

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
		for (QueueInfo info : list) {
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
		manager.register(QUEUE, false); // create queue

		for (int i = 0; i < 3; i++) {
			// add some long running tasks
			queues.add(new TestTask(i), QUEUE);
		}

		manager.start(QUEUE);

		// wait for task to execute (5s most)
		Thread.sleep(2000);

		List<QueueTask> list = queues.list(TaskState.finished, QUEUE);
		assertEquals(3, list.size());

		queues.add(new TestTask(4), QUEUE); // add one more tasks

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
		manager.register(QUEUE, false); // create queue

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
		assertTrue(list.size() >= 15);
		int count = list.size();

		// 1 was interrupted
		list = queues.list(TaskState.interrupted, QUEUE);
		assertEquals(1, list.size());

		// 4 manage to finish
		list = queues.list(TaskState.finished, QUEUE);
		assertEquals(20 - count - 1, list.size());

		// none should fail
		list = queues.list(TaskState.failed, QUEUE);
		assertEquals(0, list.size());
	}

	@Test
	public void testMultipleMachinesRunningQueues() throws InterruptedException {

		String QUEUE = "testMultipleMachinesRunningQueues";
		manager.register(QUEUE, false);

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
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE, false);

		service.add(new TimeoutTask(true), QUEUE);

		QueueInfo info = manager.info(QUEUE);
		QueueSettings settings = info.getSettings();
		settings.setTaskTimeoutSeconds(10);
		settings.setTaskInterruptTimeoutSeconds(0);

		manager.set(QUEUE, settings); // 10 seconds for tasks to time out

		manager.start(QUEUE);

		Thread.sleep(25 * 1000); // wait 20 seconds ... task should be put in failed state at least once

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

	@Test
	public void interruptTimedOutTask() throws InterruptedException {

		String QUEUE = "killTimedOutTask";
		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE, false);

		service.add(new TimeoutTask(), QUEUE);

		QueueInfo info = manager.info(QUEUE);
		QueueSettings settings = info.getSettings();
		settings.setTaskTimeoutSeconds(3);
		settings.setTaskInterruptTimeoutSeconds(0); // no gracefull period ... taks takes 3 seconds to interrupt itself

		manager.set(QUEUE, settings); // 9 seconds for tasks to time out 3 times

		manager.start(QUEUE);

		Thread.sleep(15 * 1000); // wait 15 seconds ... task should be put in failed state at least once

		// task should be in interrupted state ...
		List<QueueTask> list = queues.list(TaskState.interrupted, QUEUE);
		assertEquals(1, list.size());

		// let's purge
		QueuePurger purger = new QueuePurger(queues, QUEUE, settings);
		purger.run();

		// task will stay interrupted
		list = queues.list(TaskState.interrupted, QUEUE);
		assertEquals(1, list.size());

		list = queues.list(TaskState.running, QUEUE);
		assertEquals(0, list.size());

		list = queues.list(TaskState.failed, QUEUE);
		assertEquals(0, list.size());
	}

	@Test
	public void interruptToLongRunningTask() throws InterruptedException {

		String QUEUE = "interruptToLongRunningTask";
		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE, false);

		service.add(new LongRunningTask(), QUEUE);

		QueueInfo info = manager.info(QUEUE);
		QueueSettings settings = info.getSettings();
		settings.setTaskTimeoutSeconds(3);
		settings.setTaskInterruptTimeoutSeconds(4); // 3 seconds to finish of (it should last 3 seconds)

		manager.set(QUEUE, settings); // 10 seconds for tasks to time out

		manager.start(QUEUE);

		Thread.sleep(10 * 1000); // wait 20 seconds ... task should be put in failed state at least once

		// task should be in interrupted state ...
		List<QueueTask> list = queues.list(TaskState.interrupted, QUEUE);
		assertEquals(1, list.size());

		// let's purge
		QueuePurger purger = new QueuePurger(queues, QUEUE, settings);
		purger.run();

		// task will stay interrupted
		list = queues.list(TaskState.interrupted, QUEUE);
		assertEquals(1, list.size());

		list = queues.list(TaskState.running, QUEUE);
		assertEquals(0, list.size());

		list = queues.list(TaskState.failed, QUEUE);
		assertEquals(0, list.size());
	}

	@Test
	public void killToLongRunningTask() throws InterruptedException {

		String QUEUE = "killToLongRunningTask";
		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE, false);

		service.add(new LongRunningTask(30 * 1000, true), QUEUE);

		QueueInfo info = manager.info(QUEUE);
		QueueSettings settings = info.getSettings();
		settings.setTaskTimeoutSeconds(15);
		settings.setTaskInterruptTimeoutSeconds(5); // 10 seconds to finish of (it should last 3 seconds)

		manager.set(QUEUE, settings); // 10 seconds for tasks to time out

		manager.start(QUEUE);

		Thread.sleep(20 * 1000); // wait 25 seconds ... task should be put in failed state at least once

		// task should be in running state ...
		List<QueueTask> list = queues.list(TaskState.running, QUEUE);
		assertEquals(1, list.size());
	}

	@Test
	public void multipleThreadsOnOneManager() throws InterruptedException {

		String QUEUE = "multipleThreadsOnOneManager";
		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE, false);


		QueueInfo info = manager.info(QUEUE);
		QueueSettings settings = info.getSettings();
		settings.setTaskTimeoutSeconds(10);
		settings.setTaskInterruptTimeoutSeconds(0);

		settings.setMaxThreads(5);
		settings.setPurgeSuccessfulAfterMinutes(0);
		settings.setPurgeFailedAfterMinutes(0);

		manager.set(QUEUE, settings); // 10 seconds for tasks to time out

		for (int i = 0; i < 100; i++) {
			service.add(new LongRunningTask(1000), QUEUE); // one second for each task (on 5 threads this should take 20s)
		}


		manager.start(QUEUE);

		Thread.sleep(35 * 1000); // wait 20+ seconds ... task should be put in success

		// task should be in running state ...
		List<QueueTask> list = queues.list(TaskState.finished, QUEUE);
		assertEquals(100, list.size());

		// get some statisics
		QueuePurger purger = new QueuePurger(queues, QUEUE, settings);
		purger.run();

		info = manager.info(QUEUE);
		TaskStatistics stats = info.getStatistics(TaskState.finished);

		log.info("Min execution time: " + stats.getMinExecutionTime());
		log.info("Max execution time: " + stats.getMaxExecutionTime());
		log.info("Average execution time: " + stats.getAverageExecutionTime());
		log.info("Total execution time: " + stats.getTotalExecutionTime());

		log.info("Min job run time: " + stats.getMinJobRunTime());
		log.info("Max job run time: " + stats.getMaxJobRunTime());
		log.info("Average job run time: " + stats.getAverageJobRunTime());
		log.info("Total job run time: " + stats.getTotalJobRunTime());

		assertEquals(100, stats.getCount());
		assertTrue(stats.getMinJobRunTime() >= 1000);

		assertTrue(stats.getMaxExecutionTime() < 25 * 1000); // last task waited at least 20s ...

		// check if reset works
		manager.resetStatistics(QUEUE, false);
		info = manager.info(QUEUE);

		assertEquals(0, info.getFinishedTasks());
		assertEquals(0, info.getRunningTasks());
		assertEquals(0, info.getQueuedTasks());
		assertEquals(100, info.getPurgeTasks());

		assertEquals(0, info.getTotalTasks());
		assertEquals(0, info.getTotalRetries());
		assertEquals(0, info.getTotalFailed());
		assertEquals(0, info.getTotalFinished());

		manager.resetStatistics(QUEUE, true);
		info = manager.info(QUEUE);

		assertEquals(0, info.getPurgeTasks());
	}

	@Test
	public void multipleThreadsWithOneTimeOutTask() throws InterruptedException {

		String QUEUE = "multipleThreadsWithOneTimeOutTask";
		TaskQueueService service = new DefaultTaskQueueService(spikeify);
		TaskQueueManager manager = new DefaultTaskQueueManager(spikeify, service);
		manager.register(QUEUE, false);


		QueueInfo info = manager.info(QUEUE);
		QueueSettings settings = info.getSettings();
		settings.setTaskTimeoutSeconds(5);
		settings.setTaskInterruptTimeoutSeconds(0);

		settings.setMaxThreads(5);
		settings.setPurgeFailedAfterMinutes(10);
		settings.setPurgeSuccessfulAfterMinutes(10);

		manager.set(QUEUE, settings); // 5 seconds for tasks to time out

		for (int i = 0; i < 100; i++) {
			if (i > 5 && i <= 10) {
				service.add(new TimeoutTask(true), QUEUE); // add 5 tasks that will time out
			}

			service.add(new LongRunningTask(500, true), QUEUE); // half a second for each task (100 * 500 = 50 000 / 5 = 10 000 - on 5 threads this should take 10s)
			// + 5 * 500 = 2 500 for failing * 3 times retry = 7 500
			// total at least: 20s
		}

		manager.start(QUEUE);

		Thread.sleep(25 * 1000); // wait all time out

		info = manager.info(QUEUE);

		assertEquals(100, info.getTotalFinished()); // 100 should be finished
		assertEquals(5, info.getRunningTasks());    // 5 tasks stuck in running mode ... purge should fail them
		assertEquals(105, info.getTotalTasks());

		// get some statistics
		// set purge lower
		settings.setPurgeFailedAfterMinutes(0);
		settings.setPurgeSuccessfulAfterMinutes(0);
		settings.setTaskTimeoutSeconds(1);
		manager.set(QUEUE, settings); // 1 seconds for tasks to time out

		QueuePurger purger = new QueuePurger(queues, QUEUE, settings);
		purger.run();

		info = manager.info(QUEUE);

		assertEquals(0, info.getFinishedTasks());
		assertEquals(100, info.getPurgeTasks());
		assertEquals(100, info.getTotalFinished());
		assertEquals(5, info.getFailedTasks());
		assertEquals(105, info.getTotalTasks());

		TaskStatistics stats = info.getStatistics(TaskState.finished);

		log.info("Min execution time: " + stats.getMinExecutionTime());
		log.info("Max execution time: " + stats.getMaxExecutionTime());
		log.info("Average execution time: " + stats.getAverageExecutionTime());
		log.info("Total execution time: " + stats.getTotalExecutionTime());

		log.info("Min job run time: " + stats.getMinJobRunTime());
		log.info("Max job run time: " + stats.getMaxJobRunTime());
		log.info("Average job run time: " + stats.getAverageJobRunTime());
		log.info("Total job run time: " + stats.getTotalJobRunTime());

		assertEquals(100, stats.getCount());
		assertTrue(stats.getMinJobRunTime() >= 500);

		assertTrue(stats.getMaxExecutionTime() < 25 * 1000); // last task waited at least 20s + 5s (time out ... ) ...

		//
		List<QueueTask> list = queues.list(TaskState.failed, QUEUE);
		assertEquals(5, list.size());
	}
}