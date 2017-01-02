package com.krishagni.importer.repository;

import java.util.List;

import com.krishagni.commons.repository.Dao;
import com.krishagni.importer.domain.ImportJob;

public interface ImportJobDao extends Dao<ImportJob> {
	List<ImportJob> getImportJobs(ListImportJobsCriteria crit);
}
