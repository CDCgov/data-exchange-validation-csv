package com.me.parse_compare;

import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class QuickAndDirty {

	public static void main(final String[] args) throws Throwable {
		try {
			final ObjectReader PARSER = new CsvMapper().readerForMapOf(String.class).with(CsvSchema.emptySchema().withHeader());
			final ObjectMapper MAPPER = new ObjectMapper();

			final MappingIterator<Map<String, String>> mappingIterator = PARSER.readValues(csvContentReader());
			mappingIterator.readAll();

			while(mappingIterator.hasNext()) {
				final Map<String, String> row = mappingIterator.next();
				//final JsonNode node = MAPPER.valueToTree(row);

				//final Set<ValidationMessage> errors = schema.validate(node);
				//System.out.println(node);
			}
		} catch(final Throwable e) {
			System.out.println("error");
			e.printStackTrace();
			throw e;
		}
	}

	//	private static Reader csvSchemaReader() {
	//		final String schema = "";
	//		return new StringReader(schema);
	//	}

	private static Reader csvContentReader() {
		final String content = "a,b,c\n,,\n,,";
		return new StringReader(content);
	}

}
