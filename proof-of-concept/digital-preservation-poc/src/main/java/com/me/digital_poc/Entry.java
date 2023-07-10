package com.me.digital_poc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

import uk.gov.nationalarchives.csv.validator.api.java.CsvValidator;
import uk.gov.nationalarchives.csv.validator.api.java.FailMessage;

public class Entry {

	public static void main(final String[] args) throws Throwable {
		try {
			final String csvFilename = "src/main/resources/validatorpoc/test-elr-big.csv";
			final String csvSchemaFilename = "src/main/resources/validatorpoc/elr-schema-base.csvs";

			System.out.println("Start massageHeaders");
			final LocalDateTime massageHeadersStart = LocalDateTime.now();
			final String tempSchema = massageHeaders(csvSchemaFilename, csvFilename);
			final LocalDateTime massageHeadersEnd = LocalDateTime.now();
			System.out.println("End massageHeaders, milliseconds " + ChronoUnit.MILLIS.between(massageHeadersStart, massageHeadersEnd));

			System.out.println("Start validate");
			final LocalDateTime validateStart = LocalDateTime.now();
			final List<FailMessage> messages = CsvValidator.validate(csvFilename, tempSchema, false, new ArrayList<>(), true, false);
			final LocalDateTime validateEnd = LocalDateTime.now();
			System.out.println("End validate, milliseconds " + ChronoUnit.MILLIS.between(validateStart, validateEnd));

			for(final FailMessage error : messages) {
				System.out.println("Row " + error.getLineNumber() + " : " + error.getMessage());
			}
		} catch(final Throwable e) {
			e.printStackTrace();
			throw e;
		}
	}

	private static String massageHeaders(final String baseSchemaFilename, final String csvFilename) throws IOException {
		final File baseSchemaFile = new File(baseSchemaFilename);
		final String tempSchemaFileName = FilenameUtils.getBaseName(baseSchemaFilename)
				+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSS")) + ".csvs";
		final File tempSchemaFile = new File("target/testOutput/tempSchemas", tempSchemaFileName);
		tempSchemaFile.getParentFile().mkdirs();

		final Iterator<String> lineIter = Files.readAllLines(baseSchemaFile.toPath()).iterator();
		final List<String> linesToWrite = new ArrayList<>();

		while(lineIter.hasNext()) {
			final String readLine = lineIter.next();
			if("//START COLUMNS".contentEquals(readLine)) {
				break;
			}
			linesToWrite.add(readLine);
		}
		final Map<String, String> readMap = new HashMap<>();
		while(lineIter.hasNext()) {
			final String readLine = lineIter.next();
			if(readLine.trim().startsWith("//")) {
				continue;
			}
			final String[] splitLine = readLine.split(":", 2);
			String value;
			if(splitLine.length == 1) {
				value = "";
			} else {
				value = splitLine[1];
			}
			readMap.put(splitLine[0].trim().toLowerCase(), value);
		}

		String headerRow;
		try(BufferedReader reader = new BufferedReader(new FileReader(csvFilename))) {
			headerRow = reader.readLine();
		}

		final String[] headerRowSplit = headerRow.split(",");
		final Map<String, String> mapToReplace = new HashMap<>();
		for(final String headerField : headerRowSplit) {
			final String expectedKey = headerField.trim().toLowerCase();
			linesToWrite.add(expectedKey + ":" + readMap.get(expectedKey));
			if(!expectedKey.equals(headerField)) {
				final String replaceKeyRegex = "(?<![A-Za-z0-9\\-_\\.])" + expectedKey + "(?![A-Za-z0-9\\-_\\.])";
				mapToReplace.put(replaceKeyRegex, "\"" + headerField + "\"");
			}
		}

		String toWrite = String.join("\n", linesToWrite);
		for(final String key : mapToReplace.keySet()) {
			toWrite = toWrite.replaceAll(key, mapToReplace.get(key));
		}

		Files.write(tempSchemaFile.toPath(), toWrite.getBytes());
		return tempSchemaFile.getPath();
	}

}
