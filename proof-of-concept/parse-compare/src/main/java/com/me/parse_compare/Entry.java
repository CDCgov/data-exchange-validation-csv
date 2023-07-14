package com.me.parse_compare;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class Entry {
	private static final CSVFormat format = CSVFormat.RFC4180.withFirstRecordAsHeader();
	private static final int NUM_THREADS = 10;
	private static final int BATCH_SIZE = 10_000;

	public static void main(final String[] args) throws Throwable {
		try {
			final File testFile = getTestFile();
			final Output output = run(testFile, Entry::run_apache);
			//TODO something with output
		} catch(final Throwable e) {
			e.printStackTrace();
			throw e;
		}
	}

	private static File getTestFile() {
		// TODO Auto-generated method stub
		return null;
	}

	private static Output run(final File testFile, final Function<Reader, SubOutput> subRunner)
			throws IOException, InterruptedException, ExecutionException {
		final List<Future<SubOutput>> futures = new ArrayList<>();
		try(BufferedReader reader = Files.newBufferedReader(testFile.toPath())) {
			final String headerLine = reader.readLine();
			int currentInBatch = 0;
			StringBuilder toSubmit = new StringBuilder(headerLine);
			final ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);

			String nextLine;
			while((nextLine = reader.readLine()) != null) {
				currentInBatch++;
				toSubmit.append("\n").append(nextLine);
				if(currentInBatch > BATCH_SIZE) {
					final Reader subReader = new StringReader(toSubmit.toString());
					futures.add(service.submit(() -> subRunner.apply(subReader)));

					toSubmit = new StringBuilder(headerLine);
					currentInBatch = 0;
				}
			}
			//NOTE: since using this just for timing metrics, don't want to run the last partial batch
			//final Reader subReader = new StringReader(toSubmit.toString());
			//futures.add(service.submit(() -> subRunner.apply(subReader)));
		}

		long stillRunning = futures.size();
		while(stillRunning > 0) {
			stillRunning = futures.stream().map(Future::isDone).filter(b -> !b).count();
		}

		final int count = futures.size();
		long totalParse = 0;
		long totalParse2 = 0;
		long totalConvert = 0;
		long totalConvert2 = 0;
		for(final Future<SubOutput> future : futures) {
			final SubOutput output = future.get();
			final long parseTime = ChronoUnit.MILLIS.between(output.start, output.parsed);
			final long convertTime = ChronoUnit.MILLIS.between(output.parsed, output.converted);

			totalParse += parseTime;
			totalParse2 += parseTime * parseTime;
			totalConvert += convertTime;
			totalConvert2 += convertTime * convertTime;
		}
		final BigDecimal countBD = BigDecimal.valueOf(count).setScale(5);
		final BigDecimal avgParse = BigDecimal.valueOf(totalParse).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgParse2 = BigDecimal.valueOf(totalParse2).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgConvert = BigDecimal.valueOf(totalConvert).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgConvert2 = BigDecimal.valueOf(totalConvert2).setScale(10).divide(countBD, RoundingMode.HALF_UP);

		final BigDecimal stDevParse = avgParse2.subtract(avgParse.multiply(avgParse)).sqrt(MathContext.UNLIMITED);
		final BigDecimal stDevConvert = avgConvert2.subtract(avgConvert.multiply(avgConvert)).sqrt(MathContext.UNLIMITED);

		return new Output(avgParse, stDevParse, avgConvert, stDevConvert);
	}

	private static SubOutput run_apache(final Reader subFileReader) {
		try {
			final LocalDateTime start = LocalDateTime.now();
			final Iterable<CSVRecord> records = format.parse(subFileReader);
			final LocalDateTime parsed = LocalDateTime.now();
			//TODO convert
			final LocalDateTime converted = LocalDateTime.now();
			return new SubOutput(start, parsed, converted);
		} catch(final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static class SubOutput {
		final LocalDateTime start, parsed, converted;

		public SubOutput(final LocalDateTime start, final LocalDateTime parsed, final LocalDateTime converted) {
			this.start = start;
			this.parsed = parsed;
			this.converted = converted;
		}

	}

	private static class Output {
		final BigDecimal avgParse, stDevParse, avgConvert, stDevConvert;

		public Output(final BigDecimal avgParse, final BigDecimal stDevParse, final BigDecimal avgConvert, final BigDecimal stDevConvert) {
			this.avgParse = avgParse;
			this.stDevParse = stDevParse;
			this.avgConvert = avgConvert;
			this.stDevConvert = stDevConvert;
		}

	}
}
