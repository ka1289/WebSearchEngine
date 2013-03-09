package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import net.htmlparser.jericho.Source;

public class Parser {

	public static String parse(File file) throws IOException {
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String htmlText = "";
		StringBuilder temp = new StringBuilder();

		while ((htmlText = br.readLine()) != null) {
			Source source = new Source(htmlText);
			// System.out.println(source.getTextExtractor().toString());
			temp.append(source.getTextExtractor().toString());
		}

		return temp.toString();
	}
}
