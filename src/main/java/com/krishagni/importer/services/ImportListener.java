package com.krishagni.importer.services;

import com.krishagni.importer.domain.ImportJob;

public interface ImportListener {
	void success(ImportJob job, int totalRecs, int failedRecs);

	void fail(ImportJob job, int totalRecs, int failedRecs);
}
