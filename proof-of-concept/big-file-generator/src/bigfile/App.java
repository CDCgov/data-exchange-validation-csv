package bigfile;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class App {
	private static final String FILE_LOC = "C:/automation/NOT-AUTO/";
	private static final List<String> FILE_NAMES = Arrays.asList(FILE_LOC + "test-upload-bigzip/test-upload-bigzip-1.csv",
			FILE_LOC + "test-upload-bigzip/test-upload-bigzip-2.csv", FILE_LOC + "test-upload-bigzip/test-upload-bigzip-3.csv");
	private static final int NUM_LINES = 5_000_000;
	private static final int WORDS_PER_LINE = 100;
	private static final int CHARS_PER_WORD = 10;
	private static final String LINE_SEP = System.lineSeparator();
	private static final char WORD_SEP = ',';
	private static final List<Character> CHARS = Arrays.asList('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
			'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
			'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z');

	public static void main(final String[] args) throws FileNotFoundException, IOException {
		final Random rand = new Random();
		for(final String fileName : FILE_NAMES) {
			System.out.println("Writing " + fileName);
			try(FileOutputStream stream = new FileOutputStream(fileName)) {
				for(int l = 0 ; l < NUM_LINES ; l++) {
					final StringBuilder toBuild = new StringBuilder();

					for(int w = 0 ; w < WORDS_PER_LINE ; w++) {
						for(int c = 0 ; c < CHARS_PER_WORD ; c++) {
							toBuild.append(CHARS.get(rand.nextInt(CHARS.size())));
						}
						toBuild.append(WORD_SEP);
					}
					toBuild.deleteCharAt(toBuild.length() - 1);
					toBuild.append(LINE_SEP);
					stream.write(toBuild.toString().getBytes());
				}
			}
			System.out.println("Done Writing " + fileName);
		}
	}
}
