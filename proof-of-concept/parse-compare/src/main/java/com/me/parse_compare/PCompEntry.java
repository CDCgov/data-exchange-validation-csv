package com.me.parse_compare;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.exceptions.CsvValidationException;

public class PCompEntry {
	private static final int BATCH_SIZE = 1000;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Throwable {
		try {
			final File testFile = getTestFile();
			runJackson(testFile);
			runOpen(testFile);
			runApache(testFile);
			
		} catch(final Throwable e) {
			e.printStackTrace();
			throw e;
		}
	}

	private static File getTestFile() {
		return new File("C:\\automation\\NOT-AUTO\\test-upload-big.csv");
	}

	
	private static void runJackson(final File testFile)
			throws IOException, InterruptedException, ExecutionException {
		System.out.println("START JACKSON");
		final LocalDateTime start = LocalDateTime.now();
		

		ObjectReader parser = new CsvMapper().readerForMapOf(String.class).with(CsvSchema.emptySchema().withHeader());
		MappingIterator<Map<String, String>> mappingIterator = parser.readValues(testFile);
		
		int currentInBatch = 0;
		int counter=1;
		List<Map<String,String>> chunk = new ArrayList<>();
		while(mappingIterator.hasNext()) {
			currentInBatch++;
			chunk.add(mappingIterator.next());
			if(currentInBatch > BATCH_SIZE) {
				//PROCESS
				currentInBatch = 0;
				chunk = new ArrayList<>();
				counter++;
			}
		}
		//PROCESS

		final LocalDateTime end = LocalDateTime.now();
		long runTime=ChronoUnit.MILLIS.between(start,end);

		System.out.println("END JACKSON, # chunks "+counter+", runtime "+runTime);
	}

	
	private static void runOpen(final File testFile)
			throws IOException, InterruptedException, ExecutionException, CsvValidationException {
		System.out.println("START OPEN");
		final LocalDateTime start = LocalDateTime.now();
		
		int currentInBatch = 0;
		int counter=0;
		List<Map<String,String>> chunk = new ArrayList<>();
		try(CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(testFile))){
			Map<String,String>read;
			while((read=reader.readMap())!=null) {
				chunk.add(read);
				currentInBatch++;
				if(currentInBatch > BATCH_SIZE) {
					//PROCESS
					currentInBatch = 0;
					chunk = new ArrayList<>();
					counter++;
				}
			}
			//PROCESS
		}

		final LocalDateTime end = LocalDateTime.now();
		long runTime=ChronoUnit.MILLIS.between(start,end);


		System.out.println("END OPEN, # chunks "+counter+", runtime "+runTime);
	}


	
	private static void runApache(final File testFile)
			throws IOException, InterruptedException, ExecutionException {
		System.out.println("START APACHE");
		final LocalDateTime start = LocalDateTime.now();
		

		Iterable<CSVRecord> recordsIterable = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new FileReader(testFile));
		Iterator<CSVRecord> records=recordsIterable.iterator();
		
		int currentInBatch = 0;
		int counter=1;
		List<Map<String,String>> chunk = new ArrayList<>();
		while(records.hasNext()) {
			currentInBatch++;
			chunk.add(records.next().toMap());
			if(currentInBatch > BATCH_SIZE) {
				//PROCESS
				currentInBatch = 0;
				chunk = new ArrayList<>();
				counter++;
			}
		}
		//PROCESS

		final LocalDateTime end = LocalDateTime.now();
		long runTime=ChronoUnit.MILLIS.between(start,end);

		System.out.println("END APACHE, # chunks "+counter+", runtime "+runTime);
	}

}
