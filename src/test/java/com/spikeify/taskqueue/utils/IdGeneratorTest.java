package com.spikeify.taskqueue.utils;

import org.junit.Test;

import java.math.BigInteger;

import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;
import static org.junit.Assert.*;

public class IdGeneratorTest {

	@Test
	public void testDefinition() {

		assertUtilityClassWellDefined(IdGenerator.class);
	}

	@Test
	public void generateTest() {

		BigInteger number = IdGenerator.generate();
		BigInteger number2 = IdGenerator.generate();
		BigInteger number3 = IdGenerator.generate();
		BigInteger number4 = IdGenerator.generate();

		// numbers must be positive
		assertTrue(BigInteger.ZERO.compareTo(number) < 0);
		assertTrue(BigInteger.ZERO.compareTo(number2) < 0);
		assertTrue(BigInteger.ZERO.compareTo(number3) < 0);
		assertTrue(BigInteger.ZERO.compareTo(number4) < 0);

		// numbers should be different (at least in theory)
		assertNotEquals(number, number2);
		assertNotEquals(number2, number3);
		assertNotEquals(number3, number4);
	}

	@Test
	public void generateStringTest() {

		String id = IdGenerator.generate(1);
		assertEquals(1, id.length());
		assertTrue(IdGenerator.ELEMENTS.contains(id));

		id = IdGenerator.generate(10);
		String id2 = IdGenerator.generate(10);
		String id3 = IdGenerator.generate(10);
		String id4 = IdGenerator.generate(10);

		// check lenght
		assertEquals(10, id.length());
		assertEquals(10, id2.length());
		assertEquals(10, id3.length());
		assertEquals(10, id4.length());

		// keys should be different (at least in theory)
		assertNotEquals(id, id2);
		assertNotEquals(id3, id2);
		assertNotEquals(id4, id2);
	}
}