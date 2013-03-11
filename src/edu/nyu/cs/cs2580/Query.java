package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * Representation of a user query.
 * 
 * In HW1: instructors provide this simple implementation.
 * 
 * In HW2: students must implement {@link QueryPhrase} to handle phrases.
 * 
 * @author congyu
 * @auhtor fdiaz
 */
public class Query {
	public String _query = null;
	public Vector<String> _tokens = new Vector<String>();

	public Query(String query) {
		_query = query;
	}

	public void processQuery() {
		
		if (_query == null) {
			return;
		}
		String token = "";
		Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_30, new HashSet<String>());
		try {
	    List<String> words = tokenize(analyzer.tokenStream("", new StringReader(_query.trim())));
	    
			for (String word : words) {
				String stemmedWord = Stemmer.stemAWord(word.trim());
				token += stemmedWord + " ";
			}
			token.trim();
    } catch (IOException e) {	    
	    e.printStackTrace();
    }
		Scanner s = new Scanner(token);
		while (s.hasNext()) {
			_tokens.add(s.next());
		}
		s.close();		
	}
	
	
	static List<String> tokenize(TokenStream stream) throws IOException {
		List<String> tokens = new ArrayList<String>();
		CharTermAttribute cattr = stream.addAttribute(CharTermAttribute.class);
		while (stream.incrementToken()) {
			tokens.add(cattr.toString());
		}
		stream.end();
		stream.close();
		return tokens;

	}

}
