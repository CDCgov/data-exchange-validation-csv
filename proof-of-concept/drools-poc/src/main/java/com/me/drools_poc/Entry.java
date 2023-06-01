package com.me.drools_poc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message.Level;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.conf.MultithreadEvaluationOption;
import org.kie.internal.io.ResourceFactory;

public class Entry {

	public static void main(final String[] args) throws Throwable {
		try {
			final File testFile = buildTestFile();
			runDrools(testFile);
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

	private static void runDrools(final File testFile) throws IOException {

		System.out.println("Start createSession");
		final LocalDateTime createSessionStart = LocalDateTime.now();
		final KieSession session = createSession();
		final LocalDateTime createSessionEnd = LocalDateTime.now();
		System.out.println("End createSession, milliseconds " + ChronoUnit.MILLIS.between(createSessionStart, createSessionEnd));

		System.out.println("Start parse");
		final LocalDateTime parseStart = LocalDateTime.now();
		try(BufferedReader reader = Files.newBufferedReader(testFile.toPath())) {
			//			final String headerRow = reader.readLine();
			//			final String[] headerRowSplit = headerRow.split(",");
			final CSVFormat format = CSVFormat.RFC4180.withFirstRecordAsHeader();
			final Iterable<CSVRecord> records = format.parse(reader);

			final LocalDateTime parseEnd = LocalDateTime.now();
			System.out.println("End parse, milliseconds " + ChronoUnit.MILLIS.between(parseStart, parseEnd));

			System.out.println("Start add to execute");
			final LocalDateTime executeAddStart = LocalDateTime.now();

			for(final CSVRecord record : records) {
				session.insert(new CSVEntity(record));
			}

			final LocalDateTime executeAddEnd = LocalDateTime.now();
			System.out.println("End add to execute, milliseconds " + ChronoUnit.MILLIS.between(executeAddStart, executeAddEnd));
		}

		System.out.println("Start execute");
		final LocalDateTime executeStart = LocalDateTime.now();
		try {
			session.fireAllRules();
		} finally {
			session.dispose();
		}
		final LocalDateTime executeEnd = LocalDateTime.now();
		System.out.println("End execute, milliseconds " + ChronoUnit.MILLIS.between(executeStart, executeEnd));

		ExternalFunctions.printMessages();
	}

	private static KieSession createSession() {
		final KieServices ks = KieServices.Factory.get();
		final KieRepository kr = ks.getRepository();
		final KieFileSystem kfs = ks.newKieFileSystem();

		kfs.write("src/main/resources/drl/covid_elr.drl", ResourceFactory.newClassPathResource("drl/covid_elr.drl"));

		final KieBuilder kb = ks.newKieBuilder(kfs);

		kb.buildAll(); // kieModule is automatically deployed to KieRepository if successfully built.
		if(kb.getResults().hasMessages(Level.ERROR)) {
			throw new RuntimeException("Build Errors:\n" + kb.getResults().toString());
		}

		final KieContainer kContainer = ks.newKieContainer(kr.getDefaultReleaseId());

		final KieBaseConfiguration kieBaseConf = ks.newKieBaseConfiguration();
		kieBaseConf.setOption(MultithreadEvaluationOption.YES);
		final KieBase kieBase = kContainer.newKieBase(kieBaseConf);
		return kieBase.newKieSession();
	}

}
