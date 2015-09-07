package com.spikeify.taskqueue;

import com.aerospike.client.Host;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyService;

public class TestHelper {

	private static final Host host = new Host("127.0.0.1", 3000);
	private static final String namespace = "test";

	public static Spikeify getSpikeify() {

		if (SpikeifyService.getClient() == null) {

			SpikeifyService.globalConfig(namespace, host);
		}

		return SpikeifyService.sfy();
	}
}
