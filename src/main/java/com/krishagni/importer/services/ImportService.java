package com.krishagni.importer.services;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.krishagni.importer.events.FileRecordsDetail;
import com.krishagni.importer.events.ImportDetail;
import com.krishagni.importer.events.ImportJobDetail;
import com.krishagni.importer.events.ObjectSchemaCriteria;
import com.krishagni.importer.repository.ListImportJobsCriteria;

public interface ImportService {
	void setConfig(ImportConfig cfg);

	List<ImportJobDetail> getImportJobs(ListImportJobsCriteria req);
	
	ImportJobDetail getImportJob(Long req);
			
	String getImportJobFile(Long req);
	
	String uploadImportJobFile(InputStream in);
	
	ImportJobDetail importObjects(ImportDetail req);
	
	String getInputFileTemplate(ObjectSchemaCriteria req);

	List<Map<String, Object>> processFileRecords(FileRecordsDetail req);

	ImportJobDetail stopJob(Long req);
}
