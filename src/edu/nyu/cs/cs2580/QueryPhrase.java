package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 *          ["new york city"], the presence of the phrase "new york city" must
 *          be recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

	public QueryPhrase(String query) {
		super(query);
	}

	@Override
	public void processQuery() {
		int index = 0;
		int i = 0;
		String str = _query;
		// System.out.println(str.indexOf('"',1));
		while ((i = str.indexOf('"', index)) != -1) {
			int j = str.indexOf('"', i + 1);
			// if there is an odd no of quotes then an argument should be thrown
			if (j == -1) {
				throw new IllegalArgumentException();
			}
			// Will have to inside list only if the size>0
			String tempString = "";
			if ((tempString = str.substring(index, i)).length() > 0) {
				// This will insert the individual words split by space
				processQuery(tempString);
			}
			if ((tempString = str.substring(i, j + 1)).length() > 0) {
				// EnglisgAnalyser and Stemming of words
				Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_30, new HashSet<String>());
				List<String> words;
				String token = "";
				try {
					words = tokenize(analyzer.tokenStream("", new StringReader(tempString.trim())));
					for (String word : words) {
						String stemmedWord = Stemmer.stemAWord(word.trim());
						token += stemmedWord + " ";
					}
					token.trim();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (!token.isEmpty()) {
					_tokens.add(token);
				}
				analyzer.close();
			}
			index = j + 1;
		}
		String tempStr = str.substring(index, str.length());
		String stemmedWord = Stemmer.stemAWord(tempStr.trim());
		_tokens.add(stemmedWord);
	}

	private void processQuery(String _query) {
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
		analyzer.close();
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
