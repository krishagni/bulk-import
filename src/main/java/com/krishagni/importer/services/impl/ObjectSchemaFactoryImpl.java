package com.krishagni.importer.services.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.krishagni.commons.service.TemplateService;
import com.krishagni.importer.domain.ObjectSchema;
import com.krishagni.importer.services.ObjectSchemaBuilder;
import com.krishagni.importer.services.ObjectSchemaFactory;


public class ObjectSchemaFactoryImpl implements ObjectSchemaFactory {
	private TemplateService templateService;

	private Map<String, ObjectSchema> schemaMap = new HashMap<>();

	private Map<String, ObjectSchemaBuilder> schemaBuilders = new HashMap<>();
	
	public void setTemplateService(TemplateService templateService) {
		this.templateService = templateService;
	}

	public void setSchemaMap(Map<String, ObjectSchema> schemaMap) {
		this.schemaMap = schemaMap;
	}
	
	public void setSchemaBuilders(Map<String, ObjectSchemaBuilder> schemaBuilders) {
		this.schemaBuilders = schemaBuilders;
	}
	
	public void setSchemaResources(List<String> schemaResources) {
		for (String schemaResource : schemaResources) {
			ObjectSchema schema = parseSchema(schemaResource);
			schemaMap.put(schema.getName(), schema);
		}
	}
	
	@Override
	public ObjectSchema getSchema(String name) {
		return getSchema(name, Collections.emptyMap());
	}
	
	@Override
	public ObjectSchema getSchema(String name, Map<String, String> params) {
		ObjectSchema schema = schemaMap.get(name);
		if (schema != null) {
			return schema;
		}

		ObjectSchemaBuilder builder = schemaBuilders.get(name);
		if (builder != null) {
			schema = builder.getObjectSchema(params);
		}
		
		return schema;
	}

	@Override
	public void registerSchema(String name, ObjectSchema schema) {
		schemaMap.put(name, schema);
	}
	
	@Override
	public void registerSchema(String schemaResource) {
		ObjectSchema schema = parseSchema(schemaResource);
		schemaMap.put(schema.getName(), schema);
	}

	@Override
	public ObjectSchemaBuilder getSchemaBuilder(String name) {
		return schemaBuilders.get(name);
	}

	@Override
	public void registerSchemaBuilder(String name, ObjectSchemaBuilder builder) {
		schemaBuilders.put(name, builder);
	}

	private ObjectSchema parseSchema(String schemaResource) {
		InputStream in = null;
		try {
			in = preprocessSchema(schemaResource);
			return ObjectSchema.parseSchema(in);
		} finally {
			IOUtils.closeQuietly(in);
		}
	}
	
	private InputStream preprocessSchema(String schemaResource) {
		String template = templateService.render(schemaResource, new HashMap<String, Object>());
		return new ByteArrayInputStream( template.getBytes() );
	}
}
