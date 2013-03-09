package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import net.htmlparser.jericho.Renderer;
import net.htmlparser.jericho.Segment;
import net.htmlparser.jericho.Source;

public class Parser {

	public static String parse(File file) throws IOException {
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		StringBuilder temp = new StringBuilder();

		while ((line = br.readLine()) != null) {
			temp.append(line);
		}
		String htmlText = temp.toString();
		Source htmlSource = new Source(htmlText);
		Segment htmlSeg = new Segment(htmlSource, 0, htmlText.length());
		Renderer htmlRend = new Renderer(htmlSeg);
		br.close();
		fr.close();
		return htmlRend.toString();
	}
}
