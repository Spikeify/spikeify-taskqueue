package com.spikeify.taskqueue.entities;

import com.spikeify.Spikeify;
import com.spikeify.taskqueue.Job;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.TestHelper;
import com.spikeify.taskqueue.TestTask;
import com.spikeify.taskqueue.utils.Dummy;
import com.spikeify.taskqueue.utils.JsonUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueueTaskTest {


	private Spikeify spikeify;

	@Before
	public void setUp() {

		spikeify = TestHelper.getSpikeify();
		spikeify.truncateNamespace("test");
	}

	@After
	public void tearDown() {

		spikeify.truncateNamespace("test");
	}

	@Test
	public void createSimpleTask() {

		String QUEUE = "createSimpleTask";

		TestTask job = new TestTask(0);
		QueueTask task = new QueueTask(job, QUEUE);

		assertTrue(System.currentTimeMillis() >= task.createTime);
		assertTrue(System.currentTimeMillis() >= task.updateTime);

		assertEquals("com.spikeify.taskqueue.TestTask", task.className);
		assertEquals(TaskState.queued, task.getState());
		assertEquals(0, task.getRunCount());
		assertEquals(QUEUE, task.queue);

		// deserialize and check
		Job newJob = task.getJob();
		assertTrue(newJob instanceof TestTask);

		TestTask testJob = (TestTask) newJob;
		assertEquals(0, testJob.getProperty());
	}

	@Test(expected = TaskQueueError.class)
	public void invalidClassType() {

		String QUEUE = "invalidClassType";

		TestTask job = new TestTask(0);
		QueueTask task = new QueueTask(job, QUEUE);

		// change data of job ...
		task.className = "com.spikeify.taskqueue.utils.Dummy";

		try {
			task.getJob();
			assertTrue("Should not get to this point!", false);
		}
		catch (TaskQueueError e) {
			assertEquals("Deserialization problem: Given JSON could not be deserialized. Error: Unrecognized field \"property\" (class com.spikeify.taskqueue.utils.Dummy), not marked as ignorable (3 known properties: \"a\", \"b\", \"hidden\"])\n"
						 + " at [Source: {\"property\":0}; line: 1, column: 14] (through reference chain: com.spikeify.taskqueue.utils.Dummy[\"property\"])", e.getMessage());

			throw e;
		}
	}

	@Test(expected = TaskQueueError.class)
	public void unknownClassType() {

		String QUEUE = "unknownClassType";

		TestTask job = new TestTask(0);
		QueueTask task = new QueueTask(job, QUEUE);

		// change data of job ...
		task.className = "com.spikeify.taskqueue.Dummy";

		try {
			task.getJob();
			assertTrue("Should not get to this point!", false);
		}
		catch (TaskQueueError e) {
			assertEquals("Class 'com.spikeify.taskqueue.Dummy' not found!", e.getMessage());

			throw e;
		}
	}

	@Test(expected = TaskQueueError.class)
	public void invalidInstanceClassType() {

		String QUEUE = "invalidInstanceClassType";

		TestTask job = new TestTask(0);
		QueueTask task = new QueueTask(job, QUEUE);

		// change data of job ...
		task.className = "com.spikeify.taskqueue.utils.Dummy";
		task.job = JsonUtils.toJson(new Dummy("test", 1));

		try {
			task.getJob();
			assertTrue("Should not get to this point!", false);
		}
		catch (TaskQueueError e) {
			assertEquals("Class 'com.spikeify.taskqueue.utils.Dummy' must derive from 'com.spikeify.taskqueue.Job'!", e.getMessage());

			throw e;
		}
	}

	@Test
	public void testIsOlderThan() {

		String QUEUE = "testIsOlderThan";

		TestTask job = new TestTask(0);
		QueueTask task = new QueueTask(job, QUEUE);

		assertTrue(task.isOlderThan(0));
		assertFalse(task.isOlderThan(1));

		task.updateTime = System.currentTimeMillis() - (1000L * 60L * 10L); // 10 minutes old
		assertFalse(task.isOlderThan(11));
		assertTrue(task.isOlderThan(10));
	}

	@Test
	public void testIsOlderThanSeconds() {

	}
}