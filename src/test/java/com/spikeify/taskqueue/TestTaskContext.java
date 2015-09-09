package com.spikeify.taskqueue;

import java.util.Random;

public class TestTaskContext implements TaskContext {

	public final int count;

	public TestTaskContext() {

		Random rand = new Random();
		count = rand.nextInt(100) + 1;
	}
}
