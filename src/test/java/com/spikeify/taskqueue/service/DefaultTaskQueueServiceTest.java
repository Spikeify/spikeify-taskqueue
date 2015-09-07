package com.spikeify.taskqueue.service;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.TestHelper;
import com.spikeify.taskqueue.TestTask;
import com.spikeify.taskqueue.entities.QueueTask;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultTaskQueueServiceTest {

	private static final String QUEUE = "test";
	private TaskQueueService service;
	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();
		service = new DefaultTaskQueueService(spikeify);

		spikeify.truncateSet(QueueTask.class);
	}

	@Test
	public void testAdd() throws Exception {

		Task dummy = new TestTask("value");

		for (int i = 0; i < 5; i++) {
			// add multiple tasks to queue "test"
			service.add(dummy, QUEUE);
		}

		// list all tasks
		List<QueueTask> list = spikeify.scanAll(QueueTask.class).now();
		assertEquals(5, list.size());

		Set<String> ids = new HashSet<>();
		for (QueueTask task: list) {

			Task job = task.getTask();
			assertTrue(job instanceof TestTask);
			assertEquals(((TestTask) job).getProperty(), "value");

			assertEquals(QUEUE, task.getQueue());

			ids.add(task.getId());
		}

		assertEquals(5, ids.size());
	}

	@Test
	public void testNext() throws Exception {

		// Simple single threaded get next test ... put and get

		for (int i = 0; i < 5; i++) {
			// add multiple tasks to queue "test"
			Task dummy = new TestTask("" + i);
			service.add(dummy, QUEUE);
		}

		service.next(QUEUE);
	}

	@Test
	public void testTransition() throws Exception {

	}

	@Test
	public void testRemove() throws Exception {

	}
}