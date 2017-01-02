package com.krishagni.importer.services;

import java.util.function.Predicate;

import org.springframework.security.core.Authentication;

import com.krishagni.importer.domain.ImportJob;

public interface ImportConfig {
	String getDataDir();

	Predicate<ImportJob> isAccessAllowed();

	String getDateFmt();

	String getTimeFmt();

	Integer getAtomicitySize();

	char getFieldSeparator();

	Authentication getAuth();

	void onFinish(ImportJob job);
}
