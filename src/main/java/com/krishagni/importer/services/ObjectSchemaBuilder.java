package com.krishagni.importer.services;

import java.util.Map;

import com.krishagni.importer.domain.ObjectSchema;

public interface ObjectSchemaBuilder {
	ObjectSchema getObjectSchema(Map<String, String> params);
}
