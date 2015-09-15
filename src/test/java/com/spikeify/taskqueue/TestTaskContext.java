package com.spikeify.taskqueue;

import java.util.Random;

public class TestTaskContext implements TaskContext {

	public final int count;
	private boolean interrupt = false;

	public TestTaskContext() {

		Random rand = new Random();
		count = rand.nextInt(100) + 1;
	}

	@Override
	public boolean interrupted() {

		return interrupt;
	}

	public void interruptTask() {
		interrupt = true;
	}
}
