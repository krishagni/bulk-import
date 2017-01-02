package com.krishagni.importer.services;

import com.krishagni.importer.events.ImportObjectDetail;

public interface ObjectImporter<I, O> {
	O importObject(ImportObjectDetail<I> req);
}
