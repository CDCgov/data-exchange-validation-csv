package com.me.drools_poc;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ExternalFunctions {

	private static ConcurrentLinkedQueue<String> failList = new ConcurrentLinkedQueue<>();

	public static void fail(final CSVEntity record, final String checkName) {
		final long index = record.getRecordNumber();
		failList.add("Row " + index + " failed " + checkName);
	}

	public static void printMessages() {
		System.out.println("Number fails: " + failList.size());
	}
}
