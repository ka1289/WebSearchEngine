import java.io.BufferedReader;
package edu.nyu.cs.cs2580;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.*;
import net.htmlparser.jericho.*;

public class Parser {

	public static void parse() throws IOException {
		FileReader fr = new FileReader(
		    "C:\\sem4\\WebSearchEngine\\data\\wiki\\test.txt");
		BufferedReader br = new BufferedReader(fr);
		String htmlText = "";
		while ((htmlText = br.readLine()) != null) {//
			Source source = new Source(htmlText);
			System.out.println(source.getTextExtractor().toString());
		}
	}
}
