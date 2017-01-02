package com.krishagni.importer.services;

public interface ObjectImporterFactory {
	<I, O> ObjectImporter<I, O> getImporter(String objectType);
	
	<I, O> void registerImporter(String objectType, ObjectImporter<I, O> importer);
}
