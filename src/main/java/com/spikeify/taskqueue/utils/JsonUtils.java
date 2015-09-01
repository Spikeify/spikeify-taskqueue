package com.spikeify.taskqueue.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class JsonUtils {

	private JsonUtils() {

	}

	private static final ThreadLocal<ObjectMapper> tlObjectMapper = new ThreadLocal<ObjectMapper>() {
		@Override
		protected ObjectMapper initialValue() {

			return new ObjectMapper();
		}
	};

	/**
	 * Returns a thread-local instance of JSON ObjectMapper.
	 *
	 * @return ObjectMapper.
	 */
	public static ObjectMapper getObjectMapper() {

		return tlObjectMapper.get();
	}

	/**
	 * Converts object to JSON string
	 *
	 * @param object to be converted
	 * @return JSON representation of object
	 */
	public static String toJson(Object object) {

		try {
			return getObjectMapper().writeValueAsString(object);
		}
		catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Given Object could not be serialized to JSON. Error: " + e.getMessage());
		}
	}

	/**
	 * Allows JSON serialization with custom mapping
	 *
	 * @param object       to be serialized
	 * @param customMapper custom mapper
	 * @return JSON representation of object
	 */
	public static String toJson(Object object, ObjectMapper customMapper) {

		if (customMapper == null) {
			throw new IllegalArgumentException("Missing custom mapper!");
		}

		try {
			return customMapper.writeValueAsString(object);
		}
		catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Given Object could not be serialized to JSON. Error: " + e.getMessage());
		}
	}

	/**
	 * Transforms String to JSON object
	 *
	 * @param json      string to transform
	 * @param valueType class type
	 * @param <T>       class type
	 * @return deserialized object
	 */
	public static <T> T fromJson(String json, Class<T> valueType) {

		try {
			return getObjectMapper().readValue(json, valueType);
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Given JSON could not be deserialized. Error: " + e.getMessage());
		}
	}

	/**
	 * @param json      JSON representation of object
	 * @param reference type reference
	 * @param <T>       class type
	 * @return deserialized object
	 */
	public static <T> T fromJson(String json, TypeReference<T> reference) {

		if (reference == null) {
			throw new IllegalArgumentException("Missing type reference!");
		}

		try {
			return getObjectMapper().readValue(json, reference);
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Given JSON could not be deserialized. Error: " + e.getMessage());
		}
	}

	/**
	 * Allows de-serialization with custom mapping
	 *
	 * @param json      JSON representation of object
	 * @param reference type reference
	 * @param <T>       class type
	 * @param mapper    custom object mapper
	 * @return deserialized object
	 */
	public static <T> T fromJson(String json, TypeReference<T> reference, ObjectMapper mapper) {

		if (mapper == null) {
			throw new IllegalArgumentException("Missing object mapper!");
		}

		if (reference == null) {
			throw new IllegalArgumentException("Missing type reference!");
		}

		try {
			return mapper.readValue(json, reference);
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Given JSON could not be deserialized. Error: " + e.getMessage());
		}
	}

	/**
	 * Allows de-serialization with custom mapping
	 *
	 * @param json      JSON representation of object
	 * @param valueType class
	 * @param mapper    custom object mapper
	 * @param <T>       class type
	 * @return deserialized object
	 */
	public static <T> T fromJson(String json, Class<T> valueType, ObjectMapper mapper) {

		if (mapper == null) {
			throw new IllegalArgumentException("Missing object mapper!");
		}

		try {
			return mapper.readValue(json, valueType);
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Given JSON could not be deserialized. Error: " + e.getMessage());
		}
	}

	/**
	 * @param object    JSON representation of object
	 * @param reference type reference
	 * @param <T>       class type
	 * @return deserialized object
	 */
	public static <T> T convert(Object object, TypeReference<T> reference) {

		if (reference == null) {
			throw new IllegalArgumentException("Missing type reference!");
		}

		return getObjectMapper().convertValue(object, reference);
	}

	/**
	 * @param object    JSON representation of object
	 * @param valueType type reference
	 * @param <T>       class type
	 * @return deserialized object
	 */
	public static <T> T convert(Object object, Class<T> valueType) {

		if (valueType == null) {
			throw new IllegalArgumentException("Missing class reference!");
		}

		return getObjectMapper().convertValue(object, valueType);
	}
}
