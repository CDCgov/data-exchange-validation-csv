package com.me.drools_poc.dto;

import java.util.ArrayList;
import java.util.List;

import com.me.drools_poc.validation.IRowValidator;

public class SchemaCodeDto {
	private final List<IRowValidator> rowValidators = new ArrayList<>();

	public List<IRowValidator> getRowValidators() {
		return rowValidators;
	}
}
