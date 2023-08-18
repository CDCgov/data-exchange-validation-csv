package com.me.digital_poc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;

import uk.gov.nationalarchives.csv.validator.api.java.CsvValidator;
import uk.gov.nationalarchives.csv.validator.api.java.FailMessage;
import uk.gov.nationalarchives.csv.validator.api.java.Substitution;

public class QuickAndDirty {

	public static void main(final String[] args) throws Throwable {
		try(Reader csvContentReader = csvContentReader(); Reader csvSchemaReader = csvSchemaReader()) {

			final List<Substitution> subList = new ArrayList<>();
			final List<FailMessage> messages = CsvValidator.validate(csvContentReader, csvSchemaReader, false, subList, false, false);

			for(final FailMessage error : messages) {
				System.out.println("Row " + error.getLineNumber() + " : " + error.getMessage());
			}
		} catch(final Throwable e) {
			System.out.println("error");
			e.printStackTrace();
			throw e;
		}
	}

	private static Reader csvSchemaReader() {
		final String schema = "version 1.1\n@noHeader@totalColumns 3\n1:\n2:\n3:";
		//final String schema = "version 1.2\n\na:\nb:\nc:";
		return new StringReader(schema);
	}

	private static Reader csvContentReader() throws IOException {
		final ZipInputStream zis = new ZipInputStream(new FileInputStream("src/main/resources/validatorpoc/quick-test-bad.zip"));
		zis.getNextEntry();
		return new InputStreamReader(zis);

		//return new FileReader("src/main/resources/validatorpoc/quick-test-bad.csv");

		//		final String content = ",,\n,,\n,,";
		//		return new StringReader(content);
	}

}
