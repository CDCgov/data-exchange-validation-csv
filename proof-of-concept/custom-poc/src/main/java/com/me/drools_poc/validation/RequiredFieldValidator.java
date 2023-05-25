package com.me.drools_poc.validation;

import java.util.ArrayList;
import java.util.List;

import com.me.drools_poc.CSVEntity;
import com.me.drools_poc.dto.ValidationJsonDto;

public class RequiredFieldValidator implements IRowValidator {
	private List<String> requiredFields;

	@Override
	public String validateRow(final CSVEntity row) {
		final List<String> missing = new ArrayList<>();
		for(final String requiredField : requiredFields) {
			if(!row.isSet(requiredField)) {
				missing.add(requiredField);
			}
		}
		if(missing.isEmpty()) {
			return null;
		} else {
			return "Missing required fields " + missing.toString();
		}
	}

	@Override
	public void initialize(final ValidationJsonDto json) {
		requiredFields = (List<String>) json.getValue();
	}

}
