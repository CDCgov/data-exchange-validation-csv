package com.me.drools_poc.dto;

import java.util.List;

public class SchemaJsonDto {
	private String name;
	private List<ValidationJsonDto> validations;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public List<ValidationJsonDto> getValidations() {
		return validations;
	}

	public void setValidations(final List<ValidationJsonDto> validations) {
		this.validations = validations;
	}
}
