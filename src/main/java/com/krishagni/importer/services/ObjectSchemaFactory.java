package com.krishagni.importer.services;

import java.util.Map;

import com.krishagni.importer.domain.ObjectSchema;

public interface ObjectSchemaFactory {
	ObjectSchema getSchema(String name);
	
	ObjectSchema getSchema(String name, Map<String, String> params);
	
	void registerSchema(String schemaResource);
	
	void registerSchema(String name, ObjectSchema schema);

	ObjectSchemaBuilder getSchemaBuilder(String name);

	void registerSchemaBuilder(String name, ObjectSchemaBuilder builder);
}
