package com.spikeify.taskqueue.entities;

import com.spikeify.taskqueue.Task;
import com.spikeify.taskqueue.TaskQueueError;
import com.spikeify.taskqueue.TestTask;
import com.spikeify.taskqueue.utils.Dummy;
import com.spikeify.taskqueue.utils.JsonUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueueTaskTest {

	@Test
	public void createSimpleTask() {

		TestTask job = new TestTask("Hello task!");
		QueueTask task = new QueueTask(job);

		assertTrue(System.currentTimeMillis() >= task.created);
		assertTrue(System.currentTimeMillis() >= task.updated);

		assertEquals("com.spikeify.taskqueue.TestTask", task.className);
		assertEquals(TaskState.queued, task.getState());
		assertEquals(0, task.getRunCount());

		// deserialize and check
		Task newJob = task.getTask();
		assertTrue(newJob instanceof TestTask);

		TestTask testJob = (TestTask) newJob;
		assertEquals("Hello task!", testJob.getProperty());
	}

	@Test(expected = TaskQueueError.class)
	public void invalidClassType() {

		TestTask job = new TestTask("Hello task!");
		QueueTask task = new QueueTask(job);

		// change data of task ...
		task.className = "com.spikeify.taskqueue.utils.Dummy";

		try {
			task.getTask();
			assertTrue("Should not get to this point!", false);
		}
		catch (TaskQueueError e) {
			assertEquals("Deserialization problem: Given JSON could not be deserialized. Error: Unrecognized field \"property\" (class com.spikeify.taskqueue.utils.Dummy), not marked as ignorable (3 known properties: \"a\", \"b\", \"hidden\"])\n"
						 + " at [Source: {\"property\":\"Hello task!\"}; line: 1, column: 14] (through reference chain: com.spikeify.taskqueue.utils.Dummy[\"property\"])", e.getMessage());

			throw e;
		}
	}

	@Test(expected = TaskQueueError.class)
	public void unknownClassType() {

		TestTask job = new TestTask("Hello task!");
		QueueTask task = new QueueTask(job);

		// change data of task ...
		task.className = "com.spikeify.taskqueue.Dummy";

		try {
			task.getTask();
			assertTrue("Should not get to this point!", false);
		}
		catch (TaskQueueError e) {
			assertEquals("Class 'com.spikeify.taskqueue.Dummy' not found!", e.getMessage());

			throw e;
		}
	}

	@Test(expected = TaskQueueError.class)
	public void invalidInstanceClassType() {

		TestTask job = new TestTask("Hello task!");
		QueueTask task = new QueueTask(job);

		// change data of task ...
		task.className = "com.spikeify.taskqueue.utils.Dummy";
		task.task = JsonUtils.toJson(new Dummy("test", 1));

		try {
			task.getTask();
			assertTrue("Should not get to this point!", false);
		}
		catch (TaskQueueError e) {
			assertEquals("Class 'com.spikeify.taskqueue.utils.Dummy' must derive from 'com.spikeify.taskqueue.Task'!", e.getMessage());

			throw e;
		}
	}
}