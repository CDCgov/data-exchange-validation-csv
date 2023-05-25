package com.me.drools_poc;

import org.apache.commons.csv.CSVRecord;

public class CSVEntity {
	private final CSVRecord record;

	public CSVEntity(final CSVRecord record) {
		this.record = record;
	}

	public boolean isSet(final String header) {
		return record.isSet(header);
	}

	public long getRecordNumber() {
		return record.getRecordNumber();
	}

}
