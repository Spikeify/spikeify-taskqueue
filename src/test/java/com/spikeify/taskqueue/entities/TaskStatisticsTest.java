package com.spikeify.taskqueue.entities;

import com.spikeify.taskqueue.utils.JsonUtils;
import org.junit.*;

import static org.junit.Assert.assertEquals;

public class TaskStatisticsTest {

	@Test
	public void statisticsCalculations() {

		QueueTask task = new QueueTask();
		task.jobRunTime = 1000L;
		task.executionTime = 1100L;

		TaskStatistics.Builder builder = new TaskStatistics.Builder();
		builder.include(task);

		TaskStatistics statistics = builder.build();

		assertEquals(1L, statistics.getCount());
		assertEquals(1000L, statistics.getMaxJobRunTime());
		assertEquals(1000L, statistics.getMinJobRunTime());
		assertEquals(1000L, statistics.getAverageJobRunTime());
		assertEquals(1000L, statistics.getTotalJobRunTime());

		assertEquals(1100L, statistics.getMaxExecutionTime());
		assertEquals(1100L, statistics.getMinExecutionTime());
		assertEquals(1100L, statistics.getAverageExecutionTime());
		assertEquals(1100L, statistics.getTotalExecutionTime());


		// include next task
		task = new QueueTask();
		task.jobRunTime = 2000L;
		task.executionTime = 2200L;

		builder.include(task);
		statistics = builder.build();

		assertEquals(2L, statistics.getCount());
		assertEquals(2000L, statistics.getMaxJobRunTime());
		assertEquals(1000L, statistics.getMinJobRunTime());
		assertEquals(1500L, statistics.getAverageJobRunTime());
		assertEquals(3000L, statistics.getTotalJobRunTime());

		assertEquals(2200L, statistics.getMaxExecutionTime());
		assertEquals(1100L, statistics.getMinExecutionTime());
		assertEquals(1650L, statistics.getAverageExecutionTime());
		assertEquals(3300L, statistics.getTotalExecutionTime());

		// add second statistics to this one
		task = new QueueTask();
		task.jobRunTime = 100L;
		task.executionTime = 200L;

		TaskStatistics.Builder builder2 = new TaskStatistics.Builder();
		builder2.include(task);

		task = new QueueTask();
		task.jobRunTime = 200L;
		task.executionTime = 400L;
		builder2.include(task);

		task = new QueueTask();
		task.jobRunTime = 300L;
		task.executionTime = 600L;
		builder2.include(task);

		TaskStatistics second = builder2.build();

		assertEquals(3L, second.getCount());
		assertEquals(300L, second.getMaxJobRunTime());
		assertEquals(100L, second.getMinJobRunTime());
		assertEquals(200L, second.getAverageJobRunTime());
		assertEquals(600L, second.getTotalJobRunTime());

		assertEquals(600, second.getMaxExecutionTime());
		assertEquals(200L, second.getMinExecutionTime());
		assertEquals(400L, second.getAverageExecutionTime());
		assertEquals(1200L, second.getTotalExecutionTime());

		// Let's join those two
		TaskStatistics firstAndSecond = builder.buildWith(second);
		assertEquals(5L, firstAndSecond.getCount());
		assertEquals(2000L, firstAndSecond.getMaxJobRunTime());
		assertEquals(100L, firstAndSecond.getMinJobRunTime());

		// (1000 + 2000 + 100 + 200 + 300) / 5
		assertEquals(720L, firstAndSecond.getAverageJobRunTime());
		assertEquals(3600L, firstAndSecond.getTotalJobRunTime());

		assertEquals(2200, firstAndSecond.getMaxExecutionTime());
		assertEquals(200L, firstAndSecond.getMinExecutionTime());

		// (1100 + 2200 + 200 + 400 + 600) / 5
		assertEquals(900L, firstAndSecond.getAverageExecutionTime());
		assertEquals(4500L, firstAndSecond.getTotalExecutionTime());

		// SHOULD BE THE SAME
		firstAndSecond = builder2.buildWith(statistics);
		assertEquals(5L, firstAndSecond.getCount());
		assertEquals(2000L, firstAndSecond.getMaxJobRunTime());
		assertEquals(100L, firstAndSecond.getMinJobRunTime());

		// (1000 + 2000 + 100 + 200 + 300) / 5
		assertEquals(720L, firstAndSecond.getAverageJobRunTime());
		assertEquals(3600L, firstAndSecond.getTotalJobRunTime());

		assertEquals(2200, firstAndSecond.getMaxExecutionTime());
		assertEquals(200L, firstAndSecond.getMinExecutionTime());

		// (1100 + 2200 + 200 + 400 + 600) / 5
		assertEquals(900L, firstAndSecond.getAverageExecutionTime());
		assertEquals(4500L, firstAndSecond.getTotalExecutionTime());
	}

	@Test
	public void fromToJsonTest() {

		QueueTask task = new QueueTask();
		task.jobRunTime = 1000L;
		task.executionTime = 1100L;

		TaskStatistics.Builder builder = new TaskStatistics.Builder();
		builder.include(task);

		TaskStatistics statistics = builder.build();

		// to JSON and back
		String json = JsonUtils.toJson(statistics);
		TaskStatistics second = JsonUtils.fromJson(json, TaskStatistics.class);

		assertEquals(statistics.count, second.count);
		assertEquals(statistics.minJobRunTime, second.minJobRunTime);
		assertEquals(statistics.maxJobRunTime, second.maxJobRunTime);
		assertEquals(statistics.totalJobRunTime, second.totalJobRunTime);
		assertEquals(statistics.averageJobRunTime, second.averageJobRunTime);

		assertEquals(statistics.minExecutionTime, second.minExecutionTime);
		assertEquals(statistics.maxExecutionTime, second.maxExecutionTime);
		assertEquals(statistics.totalExecutionTime, second.totalExecutionTime);
		assertEquals(statistics.averageExecutionTime, second.averageExecutionTime);
	}
}