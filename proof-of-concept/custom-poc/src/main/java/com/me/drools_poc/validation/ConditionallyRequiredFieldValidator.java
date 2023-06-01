package com.me.drools_poc.validation;

import java.util.ArrayList;
import java.util.List;

import com.me.drools_poc.CSVEntity;
import com.me.drools_poc.dto.ValidationJsonDto;

public class ConditionallyRequiredFieldValidator implements IRowValidator {
	private List<List<String>> conditionallyRequiredFields;

	@Override
	public String validateRow(final CSVEntity row) {
		final List<String> missing = new ArrayList<>();
		for(final List<String> conditionallyRequiredFields : conditionallyRequiredFields) {
			final long numSet = conditionallyRequiredFields.stream().filter(header -> row.isSet(header)).count();
			if(numSet == 0) {
				missing.add(conditionallyRequiredFields.toString());
			}
		}
		if(missing.isEmpty()) {
			return null;
		} else {
			return "Missing conditionally required fields " + missing.toString();
		}
	}

	@Override
	public void initialize(final ValidationJsonDto json) {
		conditionallyRequiredFields = (List<List<String>>) json.getValue();
	}
}
