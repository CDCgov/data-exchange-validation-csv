package com.me.drools_poc;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.me.drools_poc.dto.SchemaCodeDto;
import com.me.drools_poc.dto.SchemaJsonDto;
import com.me.drools_poc.dto.ValidationJsonDto;
import com.me.drools_poc.validation.ConditionallyRequiredFieldValidator;
import com.me.drools_poc.validation.IRowValidator;
import com.me.drools_poc.validation.RequiredFieldValidator;

public class SchemaBuilder {

	public static SchemaCodeDto buildSchema(final SchemaJsonDto sJson) {
		final SchemaCodeDto dto = new SchemaCodeDto();
		for(final ValidationJsonDto vJson : sJson.getValidations()) {
			final ValidationMapping associatedEnum = ValidationMapping.keyMapping.get(vJson.getKey());
			//TODO some null check if bad key
			final IRowValidator validator = associatedEnum.constructor.get();
			validator.initialize(vJson);
			dto.getRowValidators().add(validator);
		}
		return dto;
	}

	//TODO in the future definitely need this to be loosely coupled
	private static enum ValidationMapping {
		REQUIRED_FIELDS("Required Fields", RequiredFieldValidator::new),
		CONDITIONALLY_REQUIRED_FIELDS("Conditionally Required Fields", ConditionallyRequiredFieldValidator::new);

		private ValidationMapping(final String key, final Supplier<IRowValidator> constructor) {
			this.key = key;
			this.constructor = constructor;
		}

		private final String key;
		private final Supplier<IRowValidator> constructor;

		private static final Map<String, ValidationMapping> keyMapping = Stream.of(values())
				.collect(Collectors.toMap(c -> c.key, Function.identity()));
	}
}
