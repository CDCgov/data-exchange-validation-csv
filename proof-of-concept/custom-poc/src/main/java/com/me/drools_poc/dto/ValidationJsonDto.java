package com.me.drools_poc.dto;

public class ValidationJsonDto {
	private String key;
	private Object value;

	public String getKey() {
		return key;
	}

	public void setKey(final String key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(final Object value) {
		this.value = value;
	}
}
