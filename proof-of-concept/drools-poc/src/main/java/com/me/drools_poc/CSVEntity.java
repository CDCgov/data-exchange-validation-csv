package com.me.drools_poc;

import org.apache.commons.csv.CSVRecord;

public class CSVEntity {
	private final CSVRecord record;

	public CSVEntity(final CSVRecord record) {
		this.record = record;
	}

	public String get(final String key) {
		return record.get(key);
	}

	public long getRecordNumber() {
		return record.getRecordNumber();
	}

}
