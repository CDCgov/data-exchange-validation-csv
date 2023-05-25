package com.me.drools_poc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.me.drools_poc.dto.SchemaCodeDto;
import com.me.drools_poc.dto.SchemaJsonDto;
import com.me.drools_poc.validation.IRowValidator;

public class CompParEntry {
	private static final CSVFormat format = CSVFormat.RFC4180.withFirstRecordAsHeader();

	public static void main(final String[] args) throws Throwable {
		try {
			final File testFile = buildTestFile();
			final File schemaFile = new File("src/main/resources/validatorpoc/elr-schema.json");
			run(testFile, schemaFile);

			ExternalFunctions.printMessages();
		} catch(final Throwable e) {
			e.printStackTrace();
			throw e;
		}

	}

	private static File buildTestFile() {
		final String csvFilename = "src/main/resources/validatorpoc/test-elr-big.csv";

		//TODO load testing
		return new File(csvFilename);
	}

	private static void log(final String message) {
		System.out.println(Thread.currentThread().getName() + ": " + message);
	}

	private static void run(final File testFile, final File schemaFile) throws IOException, InterruptedException {

		log("Start schemaParse");
		final LocalDateTime schemaParseStart = LocalDateTime.now();
		SchemaJsonDto sJson;
		try(Reader reader = new FileReader(schemaFile)) {
			sJson = GsonWrapper.gson().fromJson(reader, SchemaJsonDto.class);
		}
		final LocalDateTime schemaParseEnd = LocalDateTime.now();
		log("End schemaParse, milliseconds " + ChronoUnit.MILLIS.between(schemaParseStart, schemaParseEnd));

		log("Start schemaBuild");
		final LocalDateTime schemaBuildStart = LocalDateTime.now();
		final SchemaCodeDto schema = SchemaBuilder.buildSchema(sJson);
		final LocalDateTime schemaBuildEnd = LocalDateTime.now();
		log("End schemaBuild, milliseconds " + ChronoUnit.MILLIS.between(schemaBuildStart, schemaBuildEnd));

		log("Start split-execute");
		final LocalDateTime splitStart = LocalDateTime.now();
		final List<Future<?>> futures = new ArrayList<>();
		try(BufferedReader reader = Files.newBufferedReader(testFile.toPath())) {

			final String headerLine = reader.readLine();
			int currentInBatch = 0;
			StringBuilder toSubmit = new StringBuilder(headerLine);
			final ExecutorService service = Executors.newFixedThreadPool(10);

			String nextLine;
			while((nextLine = reader.readLine()) != null) {
				currentInBatch++;
				toSubmit.append("\n").append(nextLine);
				if(currentInBatch > 10000) {
					final Reader subReader = new StringReader(toSubmit.toString());
					futures.add(service.submit(() -> runSubFile(subReader, schema)));

					toSubmit = new StringBuilder(headerLine);
					currentInBatch = 0;
				}
			}
			final Reader subReader = new StringReader(toSubmit.toString());
			futures.add(service.submit(() -> runSubFile(subReader, schema)));
		}

		final LocalDateTime splitEnd = LocalDateTime.now();
		log("End split-execute, milliseconds " + ChronoUnit.MILLIS.between(splitStart, splitEnd));

		log("Start execute-wait");
		final LocalDateTime executeWaitStart = LocalDateTime.now();
		long stillRunning = futures.size();
		while(stillRunning > 0) {
			stillRunning = futures.stream().map(Future::isDone).filter(b -> !b).count();
		}
		final LocalDateTime executeWaitEnd = LocalDateTime.now();
		log("End execute-wait, milliseconds " + ChronoUnit.MILLIS.between(executeWaitStart, executeWaitEnd));
	}

	private static void runSubFile(final Reader subFileReader, final SchemaCodeDto schema) {
		try {
			//log("Start sub parse");
			//final LocalDateTime parseStart = LocalDateTime.now();
			final Iterable<CSVRecord> records = format.parse(subFileReader);
			//final LocalDateTime parseEnd = LocalDateTime.now();
			//log("End sub parse, milliseconds " + ChronoUnit.MILLIS.between(parseStart, parseEnd));

			//log("Start sub execute");
			//final LocalDateTime executeStart = LocalDateTime.now();
			executeSubFile(records, schema);
			//final LocalDateTime executeEnd = LocalDateTime.now();
			//log("End sub execute, milliseconds " + ChronoUnit.MILLIS.between(executeStart, executeEnd));
		} catch(final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void executeSubFile(final Iterable<CSVRecord> records, final SchemaCodeDto schema) throws InterruptedException {
		//log("Start sub execute-build");
		//final LocalDateTime executeStart = LocalDateTime.now();
		final ExecutorService service = Executors.newFixedThreadPool(5);

		final List<Future<?>> futures = new ArrayList<>();

		for(final CSVRecord record : records) {
			futures.add(service.submit(() -> runRecord(new CSVEntity(record), schema)));
		}
		//final LocalDateTime executeEnd = LocalDateTime.now();
		//log("End sub execute-build, milliseconds " + ChronoUnit.MILLIS.between(executeStart, executeEnd));

		//log("Start sub execute-wait");
		//final LocalDateTime executeWaitStart = LocalDateTime.now();
		long stillRunning = futures.size();
		while(stillRunning > 0) {
			stillRunning = futures.stream().map(Future::isDone).filter(b -> !b).count();
		}
		//final LocalDateTime executeWaitEnd = LocalDateTime.now();
		//log("End sub execute-wait, milliseconds " + ChronoUnit.MILLIS.between(executeWaitStart, executeWaitEnd));

	}

	private static void runRecord(final CSVEntity record, final SchemaCodeDto schema) {
		for(final IRowValidator validator : schema.getRowValidators()) {
			final String error = validator.validateRow(record);
			if(error != null) {
				ExternalFunctions.fail(record, error);
			}
		}
	}
}
