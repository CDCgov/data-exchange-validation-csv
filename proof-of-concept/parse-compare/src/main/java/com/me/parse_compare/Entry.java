package com.me.parse_compare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

public class Entry {
	private static final int NUM_THREADS = 10;
	private static final int BATCH_SIZE = 10000;

	private static final ObjectReader PARSER = new CsvMapper().readerForMapOf(String.class).with(CsvSchema.emptySchema().withHeader());
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Throwable {
		try {
			final JsonSchema schema = getTestSchema();
			final File testFile = getTestFile();
			final Output output = run(testFile, schema);
			
			System.out.println(MAPPER.writeValueAsString(output));
		} catch(final Throwable e) {
			e.printStackTrace();
			throw e;
		}
	}

	private static JsonSchema getTestSchema() throws FileNotFoundException, IOException {
		String[] headers;
		try(BufferedReader reader = new BufferedReader(new FileReader(getTestFile()))){
			headers=reader.readLine().split(",");
		}
		
		StringBuilder schemaString = new StringBuilder("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\": \"object\",\"properties\": {");
		for(String header:headers) {
			schemaString.append("\""+header+"\":{\"type\":\"string\"},");
		}
		schemaString.deleteCharAt(schemaString.length()-1);
		schemaString.append("}");
		schemaString.append(", \"allOf\":[{\"required\":[\"FB65yPvXi4\",\"RorP8K5P5S\"]},{\"anyOf\":[{\"required\":[\"0cpBaTtOxG\"]},{\"required\":[\"7NEXwv4jtk\"]}]}]");
		schemaString.append("}");
		
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        return factory.getSchema(schemaString.toString());
	}

	private static File getTestFile() {
		return new File("C:\\automation\\NOT-AUTO\\test-upload-big.csv");
	}

	private static Output run(final File testFile, JsonSchema schema)
			throws IOException, InterruptedException, ExecutionException {
		System.out.println("START");
		final LocalDateTime start = LocalDateTime.now();
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
					futures.add(service.submit(() -> subRunner(subReader, schema)));

					toSubmit = new StringBuilder(headerLine);
					currentInBatch = 0;
				}
			}
			//NOTE: since using this just for timing metrics, don't want to run the last partial batch
			//final Reader subReader = new StringReader(toSubmit.toString());
			//futures.add(service.submit(() -> subRunner(subReader, schema)));
		}

		long stillRunning = futures.size();
		while(stillRunning > 0) {
			stillRunning = futures.stream().map(Future::isDone).filter(b -> !b).count();
		}
		final LocalDateTime end = LocalDateTime.now();
		long runTime=ChronoUnit.MILLIS.between(start,end);

		final int count = futures.size();
		long totalParse = 0;
		long totalParse2 = 0;
		long totalConvert = 0;
		long totalConvert2 = 0;
		long totalValid = 0;
		long totalValid2 = 0;
		for(final Future<SubOutput> future : futures) {
			final SubOutput output = future.get();

			totalParse += output.parseTime;
			totalParse2 += output.parseTime * output.parseTime;
			totalConvert += output.convertTime;
			totalConvert2 += output.convertTime * output.convertTime;
			totalValid += output.validTime;
			totalValid2 += output.validTime * output.validTime;
		}
		final BigDecimal countBD = BigDecimal.valueOf(count).setScale(10);
		final BigDecimal avgParse = BigDecimal.valueOf(totalParse).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgParse2 = BigDecimal.valueOf(totalParse2).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgConvert = BigDecimal.valueOf(totalConvert).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgConvert2 = BigDecimal.valueOf(totalConvert2).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgValid = BigDecimal.valueOf(totalValid).setScale(10).divide(countBD, RoundingMode.HALF_UP);
		final BigDecimal avgValid2 = BigDecimal.valueOf(totalValid2).setScale(10).divide(countBD, RoundingMode.HALF_UP);

		final BigDecimal stDevParse = avgParse2.subtract(avgParse.multiply(avgParse)).sqrt(MathContext.DECIMAL64);
		final BigDecimal stDevConvert = avgConvert2.subtract(avgConvert.multiply(avgConvert)).sqrt(MathContext.DECIMAL64);
		final BigDecimal stDevValid = avgValid2.subtract(avgValid.multiply(avgValid)).sqrt(MathContext.DECIMAL64);

		return new Output(runTime, avgParse, stDevParse, avgConvert, stDevConvert, avgValid, stDevValid);
	}

	private static SubOutput subRunner(final Reader subFileReader, JsonSchema schema) {
		try {
			final LocalDateTime start = LocalDateTime.now();
			
			MappingIterator<Map<String, String>> mappingIterator = PARSER.readValues(subFileReader);
			List<Map<String, String>> list = mappingIterator.readAll();
			final LocalDateTime parsed = LocalDateTime.now();
			
			JsonNode node = MAPPER.valueToTree(list);
			final LocalDateTime converted = LocalDateTime.now();
			
			Set<ValidationMessage> errors = schema.validate(node);
			final LocalDateTime validated = LocalDateTime.now();
			
			return new SubOutput(start, parsed, converted, validated);
		} catch(final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static class SubOutput {
		private final long parseTime, convertTime, validTime;

		public SubOutput(final LocalDateTime start, final LocalDateTime parsed, final LocalDateTime converted, final LocalDateTime validated) {
			this.parseTime = ChronoUnit.MILLIS.between(start, parsed);
			this.convertTime = ChronoUnit.MILLIS.between(parsed, converted);
			this.validTime = ChronoUnit.MILLIS.between(converted, validated);
		}

		public long getParseTime() {
			return parseTime;
		}

		public long getConvertTime() {
			return convertTime;
		}

		public long getValidTime() {
			return validTime;
		}


	}

	public static class Output {
		private final long runTime;
		private final BigDecimal avgParse, stDevParse, avgConvert, stDevConvert, avgValid, stDevValid;

		public Output(final long runTime, final BigDecimal avgParse, final BigDecimal stDevParse, final BigDecimal avgConvert, final BigDecimal stDevConvert, final BigDecimal avgValid, final BigDecimal stDevValid) {
			this.runTime = runTime;
			this.avgParse = avgParse;
			this.stDevParse = stDevParse;
			this.avgConvert = avgConvert;
			this.stDevConvert = stDevConvert;
			this.avgValid = avgValid;
			this.stDevValid = stDevValid;
		}

		public long getRunTime() {
			return runTime;
		}

		public BigDecimal getAvgParse() {
			return avgParse;
		}

		public BigDecimal getStDevParse() {
			return stDevParse;
		}

		public BigDecimal getAvgConvert() {
			return avgConvert;
		}

		public BigDecimal getStDevConvert() {
			return stDevConvert;
		}

		public BigDecimal getAvgValid() {
			return avgValid;
		}

		public BigDecimal getStDevValid() {
			return stDevValid;
		}
		
		
	}
}
