package com.spikeify.taskqueue.utils;

import org.junit.Test;

import java.util.*;

import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;
import static org.junit.Assert.*;

public class StringUtilsTest {

	@Test
	public void testDefinition() {

		assertUtilityClassWellDefined(StringUtils.class);
	}

	@Test
	public void testEquals() {

		assertTrue(StringUtils.equals(null, null));
		assertFalse(StringUtils.equals(null, ""));
		assertFalse(StringUtils.equals("", null));
		assertTrue(StringUtils.equals("", ""));

		assertTrue(StringUtils.equals("A", "A"));
		assertFalse(StringUtils.equals("A", "a"));
	}

	@Test
	public void testEqualsIgnoreCase() {

		assertTrue(StringUtils.equals(null, null, true));
		assertFalse(StringUtils.equals(null, "", true));
		assertFalse(StringUtils.equals("", null, true));
		assertTrue(StringUtils.equals("", "", true));

		assertTrue(StringUtils.equals("A", "A", true));
		assertTrue(StringUtils.equals("A", "a", true));
	}

	@Test
	public void testCompare() {

		assertEquals(0, StringUtils.compare(null, null));
		assertEquals(1, StringUtils.compare("", null));
		assertEquals(-1, StringUtils.compare(null, ""));
		assertEquals(0, StringUtils.compare("", ""));
		assertEquals(0, StringUtils.compare("a", "a"));
		assertEquals(1, StringUtils.compare("aa", "a"));
	}

	@Test
	public void testTrim() {

		assertNull(StringUtils.trim(null));
		assertEquals("", StringUtils.trim(""));
		assertEquals("a", StringUtils.trim(" a "));
	}

	@Test
	public void testTrimToNull() {

		assertNull(StringUtils.trimToNull(null));
		assertNull(StringUtils.trimToNull(""));
		assertEquals("a", StringUtils.trimToNull(" a "));
	}

	@Test
	public void testRemoveDoubleSpaces() {

		assertEquals(null, StringUtils.trimDoubleSpaces(null));
		assertEquals("", StringUtils.trimDoubleSpaces(""));
		assertEquals("", StringUtils.trimDoubleSpaces(" "));
		assertEquals("plast. vreć. 1x500 m", StringUtils.trimDoubleSpaces(" plast.   vreć.   1x500   m  "));
		assertEquals("", StringUtils.trimDoubleSpaces(" "));
		assertEquals("a", StringUtils.trimDoubleSpaces("  a   "));
		assertEquals("a a", StringUtils.trimDoubleSpaces("  a a  "));
		assertEquals("a b c", StringUtils.trimDoubleSpaces(" a    b     c  "));
	}

	@Test
	public void testRemoveSpaces() {

		assertEquals(null, StringUtils.trimInner(null));
		assertEquals("", StringUtils.trimInner(""));
		assertEquals("", StringUtils.trimInner(" "));
		assertEquals("a", StringUtils.trimInner("  a   "));
		assertEquals("aa", StringUtils.trimInner("  a a  "));
		assertEquals("abc", StringUtils.trimInner(" a    b     c  "));
	}

	@Test
	public void testTrimEnd() {

		assertEquals(null, StringUtils.trimEnd(null));
		assertEquals("", StringUtils.trimEnd(""));
		assertEquals("", StringUtils.trimEnd(" "));
		assertEquals("  a", StringUtils.trimEnd("  a   "));
		assertEquals("  a a", StringUtils.trimEnd("  a a  "));
		assertEquals(" a    b     c", StringUtils.trimEnd(" a    b     c  "));
	}

	@Test
	public void testTrimStart() {

		assertEquals(null, StringUtils.trimStart(null));
		assertEquals("", StringUtils.trimStart(""));
		assertEquals("", StringUtils.trimStart(" "));
		assertEquals("a   ", StringUtils.trimStart("  a   "));
		assertEquals("a a  ", StringUtils.trimStart("  a a  "));
		assertEquals("a    b     c  ", StringUtils.trimStart("              a    b     c  "));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoin_Fail() {

		try {
			Set<String> set = null;
			StringUtils.join(set, null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing separator!", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testJoin() {

		Set<String> set = new HashSet<>();
		assertEquals("", StringUtils.join(set, ","));

		set.add("A");
		assertEquals("A", StringUtils.join(set, ","));

		set.add("B");
		assertEquals("A,B", StringUtils.join(set, ","));

		assertEquals("A, B", StringUtils.join(set, ", "));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoin_Fail2() {

		try {
			List<String> list = null;
			StringUtils.join(list, null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing separator!", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testJoin2() {

		List<String> list = new ArrayList<>();
		assertEquals("", StringUtils.join(list, ","));

		list.add("A");
		assertEquals("A", StringUtils.join(list, ","));

		list.add("B");
		assertEquals("A,B", StringUtils.join(list, ","));

		assertEquals("A, B", StringUtils.join(list, ", "));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoin_Fail3() {

		try {
			String[] array = null;
			StringUtils.join(array, null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing separator!", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testJoin3() {

		String[] array = new String[] {};
		assertEquals("", StringUtils.join(array, ","));

		array = new String[] {"A"};
		assertEquals("A", StringUtils.join(array, ","));

		array = new String[] {"A", "B"};
		assertEquals("A,B", StringUtils.join(array, ","));

		assertEquals("A, B", StringUtils.join(array, ", "));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testJoin_Fail4() {

		try {
			HashMap<String, String> set = null;
			StringUtils.join(set, null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing separator!", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testJoin4() {

		HashMap<String, String> set = new LinkedHashMap<>();
		assertEquals("", StringUtils.join(set, ","));

		set.put("A", "1");
		assertEquals("A=1", StringUtils.join(set, ","));

		set.put("B", "2");
		assertEquals("A=1,B=2", StringUtils.join(set, ","));

		assertEquals("A=1, B=2", StringUtils.join(set, ", "));
	}

	@Test
	public void getWordsFromTextTest() {

		List<String> output = StringUtils.getWords(null);
		assertEquals(0, output.size());

		output = StringUtils.getWords("");
		assertEquals(0, output.size());

		output = StringUtils.getWords("         ");
		assertEquals(0, output.size());

		output = StringUtils.getWords(" abra kadabra");
		assertEquals(2, output.size());
		assertEquals("abra", output.get(0));
		assertEquals("kadabra", output.get(1));


		output = StringUtils.getWords(" abra, ::(12313) kadabra!");
		assertEquals(2, output.size());
		assertEquals("abra", output.get(0));
		assertEquals("kadabra", output.get(1));

		output = StringUtils.getWords(" rdeče češnje rastejo na želvi");
		assertEquals(5, output.size());
		assertEquals("rdeče", output.get(0));
		assertEquals("češnje", output.get(1));
		assertEquals("rastejo", output.get(2));
		assertEquals("na", output.get(3));
		assertEquals("želvi", output.get(4));
	}

	@Test
	public void trimAllTest() {

		assertNull(StringUtils.trimAll(null, null));
		assertNull(StringUtils.trimAll(null, ""));
		assertNull(StringUtils.trimAll(null, "X"));

		assertEquals("", StringUtils.trimAll("", null));
		assertEquals("A", StringUtils.trimAll("A", null));

		assertEquals("aa", StringUtils.trimAll("AaAaA", "A"));
		assertEquals("AaAaA", StringUtils.trimAll("A-a-Aa-A", "-"));
	}

	@Test
	public void trimTextDownTest() {

		assertEquals("T", StringUtils.trimTextDown("Text", 1));

		assertEquals("Text", StringUtils.trimTextDown("Text", 4));
		assertEquals("Text", StringUtils.trimTextDown("Text", 5));

		assertEquals("Text", StringUtils.trimTextDown("Text to be trimmed down", 5));
		assertEquals("Text to be trimmed", StringUtils.trimTextDown("Text to be trimmed down", 22));
		assertEquals("Text to be trimmed down", StringUtils.trimTextDown("Text to be trimmed down", 25));
	}

	@Test
	public void toStringOrNullTest() {

		assertNull(StringUtils.toStringOrNull(null));
		assertEquals("", StringUtils.toStringOrNull(""));

		String test = "test";
		assertEquals("test", StringUtils.toStringOrNull(test));
	}

	@Test
	public void getListOfCharsTest() {

		List<String> list = StringUtils.asListOfChars(null);
		assertEquals(0, list.size());

		list = StringUtils.asListOfChars("a");
		assertEquals(1, list.size());
		assertEquals("a", list.get(0));

		list = StringUtils.asListOfChars("abc");
		assertEquals(3, list.size());
		assertEquals("a", list.get(0));
		assertEquals("b", list.get(1));
		assertEquals("c", list.get(2));
	}

	@Test
	public void isWordTest() {

		assertFalse(StringUtils.isWord(null));
		assertFalse(StringUtils.isWord(""));
		assertFalse(StringUtils.isWord("  "));
		assertFalse(StringUtils.isWord(" . "));
		assertFalse(StringUtils.isWord(" , "));
		assertFalse(StringUtils.isWord("test,me"));
		assertFalse(StringUtils.isWord("   !pussy-cat!  "));

		assertTrue(StringUtils.isWord("test"));
		assertTrue(StringUtils.isWord("   me  "));
		assertTrue(StringUtils.isWord("   me,  "));
		assertTrue(StringUtils.isWord("   me!  "));
		assertTrue(StringUtils.isWord("   !HELLO!  "));
		assertTrue(StringUtils.isWord("   Češka  "));
	}
}