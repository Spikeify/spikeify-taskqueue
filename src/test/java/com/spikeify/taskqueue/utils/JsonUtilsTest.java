package com.spikeify.taskqueue.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.ArrayList;

import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;
import static org.junit.Assert.*;

public class JsonUtilsTest {

	@Test
	public void testDefinition() {

		assertUtilityClassWellDefined(JsonUtils.class);
	}

	@Test
	public void testGetObjectMapper() {

		assertNotNull(JsonUtils.getObjectMapper());
	}

	@Test
	public void testToJson() {

		Dummy test = new Dummy("1", 2);
		assertEquals("{\"a\":\"1\",\"b\":2,\"hidden\":0}", JsonUtils.toJson(test));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testToJsonCustomFail() {

		Dummy test = new Dummy("1", 2);
		try {
			JsonUtils.toJson(test, null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing custom mapper!", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testToJsonCustom() {

		Dummy test = new Dummy("1", 2);
		ObjectMapper custom = new ObjectMapper();
		assertEquals("{\"a\":\"1\",\"b\":2,\"hidden\":0}", JsonUtils.toJson(test, custom));
	}

	@Test
	public void testFromJson() {

		Dummy test = JsonUtils.fromJson("{\"a\":\"1\",\"b\":2}", Dummy.class);
		assertEquals("1", test.a);
		assertEquals(2, test.b);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromJsonFail() {

		try {
			JsonUtils.fromJson("[\"a\":1,\"b\":2]", Dummy.class);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Given JSON could not be deserialized. Error: Can not deserialize instance of com.spikeify.taskqueue.utils.Dummy out of START_ARRAY token\n"
						 + " at [Source: [\"a\":1,\"b\":2]; line: 1, column: 1]", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testFromJsonTypeReference() {

		ArrayList<Dummy> list = JsonUtils.fromJson("[{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]", new TypeReference<ArrayList<Dummy>>() {});
		assertEquals(2, list.size());
		assertEquals("1", list.get(0).a);
		assertEquals(2, list.get(1).b);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromJsonTypeReferenceFail() {

		try {
			TypeReference<Object> reference = null;
			JsonUtils.fromJson("[{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]", reference);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing type reference!", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromJsonTypeReferenceFail2() {

		try {
			JsonUtils.fromJson("[{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]", new TypeReference<ArrayList<DummyTo>>() {});
		}
		catch (IllegalArgumentException e) {
			assertEquals("Given JSON could not be deserialized. Error: No suitable constructor found for type [simple type, class com.spikeify.taskqueue.utils.JsonUtilsTest$DummyTo]: can not instantiate from JSON object (missing default constructor or creator, or perhaps need to add/enable type information?)\n"
						 + " at [Source: [{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]; line: 1, column: 3] (through reference chain: java.util.ArrayList[0])",
						 e.getMessage());
			throw e;
		}
	}

	@Test
	public void testFromJsonReferenceAndCustomMapper() {

		ObjectMapper custom = new ObjectMapper();
		ArrayList<Dummy> list = JsonUtils.fromJson("[{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]", new TypeReference<ArrayList<Dummy>>() {}, custom);
		assertEquals(2, list.size());
		assertEquals("1", list.get(0).a);
		assertEquals(2, list.get(1).b);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromJsonReferenceAndCustomMapperFail() {

		try {
			JsonUtils.fromJson("[{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]", new TypeReference<ArrayList<Dummy>>() {}, null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing object mapper!", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromJsonReferenceAndCustomMapperFail_2() {

		ObjectMapper custom = new ObjectMapper();
		TypeReference<Object> reference = null;

		try {
			JsonUtils.fromJson("[{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]", reference, custom);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing type reference!", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromJsonReferenceAndCustomMapperFail_3() {

		ObjectMapper custom = new ObjectMapper();

		try {
			JsonUtils.fromJson("[{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]", new TypeReference<ArrayList<DummyTo>>() {}, custom);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Given JSON could not be deserialized. Error: No suitable constructor found for type [simple type, class com.spikeify.taskqueue.utils.JsonUtilsTest$DummyTo]: can not instantiate from JSON object (missing default constructor or creator, or perhaps need to add/enable type information?)\n"
						 + " at [Source: [{\"a\":\"1\",\"b\":2},{\"a\":\"1\",\"b\":2}]; line: 1, column: 3] (through reference chain: java.util.ArrayList[0])", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testFromJsonCustomMapper() {

		Dummy value = JsonUtils.fromJson("{\"a\":\"1\",\"b\":2}", Dummy.class, new ObjectMapper());
		assertEquals("1", value.a);
		assertEquals(2, value.b);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromJsonCustomMapperFail() {

		try {
			JsonUtils.fromJson("{\"a\":\"1\",\"b\":2}", Dummy.class, null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Missing object mapper!", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void convertFail() {

		TypeReference<String> ref = null;

		try {
			JsonUtils.convert("bla", ref);
		}
		catch (IllegalArgumentException e)
		{
			assertEquals("Missing type reference!", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void convertFail_2() {

		Class<String> ref = null;

		try {
			JsonUtils.convert("bla", ref);
		}
		catch (IllegalArgumentException e)
		{
			assertEquals("Missing class reference!", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void fromJsonFailTest() {

		ObjectMapper mapper = JsonUtils.getObjectMapper();
		try {
			JsonUtils.fromJson("BLA", String.class, mapper);
		}
		catch (IllegalArgumentException e) {
			assertEquals("Given JSON could not be deserialized. Error: Unrecognized token 'BLA': was expecting ('true', 'false' or 'null')\n"
						 + " at [Source: BLA; line: 1, column: 7]", e.getMessage());
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void toJsonFailTest() {

		try {
			InputStream stream = Mockito.mock(InputStream.class);
			JsonUtils.toJson(stream);
		}
		catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().startsWith("Given Object could not be serialized to JSON."));
			throw e;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void toJsonFailTest_2() {

		ObjectMapper mapper = JsonUtils.getObjectMapper();
		try {
			InputStream stream = Mockito.mock(InputStream.class);
			JsonUtils.toJson(stream, mapper);
		}
		catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().startsWith("Given Object could not be serialized to JSON."));
			throw e;
		}
	}

	class DummyTo {}
}