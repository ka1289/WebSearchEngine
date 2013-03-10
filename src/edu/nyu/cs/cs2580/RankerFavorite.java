package edu.nyu.cs.cs2580;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2 based on a refactoring of your favorite
 *          Ranker (except RankerPhrase) from HW1. The new Ranker should no
 *          longer rely on the instructors' {@link IndexerFullScan}, instead it
 *          should use one of your more efficient implementations.
 */
public class RankerFavorite extends Ranker {
	private Indexer _index;

	public RankerFavorite(Options options, CgiArguments arguments, Indexer indexer) {
		super(options, arguments, indexer);
		System.out.println("Using Ranker: " + this.getClass().getSimpleName());
	}

	@Override
	public Vector<ScoredDocument> runQuery(Query query, int numResults) {
		// String q = query._query;
		// Scanner s = new Scanner(q);
		// Vector<String> qv = new Vector<String>();
		// while (s.hasNext()) {
		// String term = s.next();
		// qv.add(term);
		// }
		// Vector<ScoredDocument> retrieval_results = new
		// Vector<ScoredDocument>();
		// for (int i = 0; i < _index.numDocs(); ++i) {
		// preprocess(i);
		// Document doc = _index.getDoc(i);
		// retrieval_results.add(runquery_QL(qv, i));
		//
		// }
		// s.close();
		// ScoredDocument[] array = retrieval_results.toArray(new
		// ScoredDocument[retrieval_results.size()]);
		// Arrays.sort(array);
		// retrieval_results = new Vector<ScoredDocument>(Arrays.asList(array));
		// return retrieval_results;
		return null;
	}

	// private void preprocess(int did) {
	//
	// Document d = _index.getDoc(did);
	// HashMap<String, Integer> mapOfWordCounts = new HashMap<String,
	// Integer>();
	//
	// Vector<String> v = d.get_title_vector();
	// v.addAll(d.get_body_vector());
	//
	// for (String word : v) {
	// corpusTerms.add(word);
	// if (mapOfWordCounts.containsKey(word)) {
	// int count = mapOfWordCounts.get(word);
	// count++;
	// mapOfWordCounts.put(word, count);
	// } else {
	// mapOfWordCounts.put(word, 1);
	// }
	// }
	//
	// mapOfDocuments.put(did, mapOfWordCounts);
	// }
	//
	// protected long getNumOfOccurrences(String word, int did) {
	// int count = 0;
	// for (String s : _index.getDoc(did).get_body_vector()) {
	// if (s.equals(word))
	// count++;
	// }
	// for (String s : _index.getDoc(did).get_title_vector()) {
	// if (s.equals(word))
	// count++;
	// }
	// return count;
	// }
	//
	// protected ScoredDocument runquery_QL(Vector<String> query, int did) {
	// DocumentIndexed document = (DocumentIndexed) _indexer
	// .getDoc(did);
	//
	// double score = 0;
	// double lambda = 0.5;
	// for (String q : query) {
	// double docFreq = (1.0 * getNumOfOccurrences(q, did)) / (1.0 *
	// document.getTotalWords());
	// double collectionFreq = (1.0 * _index.termFrequency(q)) / (1.0 *
	// _index.termFrequency());
	// double tmp = ((1 - lambda) * (docFreq)) + (lambda * (collectionFreq));
	// score += (Math.log(tmp) / Math.log(2));
	// }
	// return new ScoredDocument(did, document.get_title_string(), Math.pow(2,
	// score));
	// }
}
