package com.me.drools_poc.validation;

import com.me.drools_poc.CSVEntity;
import com.me.drools_poc.dto.ValidationJsonDto;

public interface IRowValidator {
	public String validateRow(CSVEntity row);

	public void initialize(ValidationJsonDto json);
}
