package com.krishagni.importer.services;

import org.springframework.security.core.Authentication;

import com.krishagni.importer.domain.ImportJob;

public interface ImportConfig {
	String getDataDir();

	boolean isAccessAllowed(ImportJob job);

	String getDateFmt();

	String getTimeFmt();

	Integer getAtomicitySize();

	char getFieldSeparator();

	Authentication getAuth();

	void onFinish(ImportJob job);
}
