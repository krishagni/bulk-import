package com.krishagni.importer.events;

public interface Mergeable<K, V> {
	K getMergeKey();

	void merge(V other);
}