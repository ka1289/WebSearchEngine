package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
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
		query.processQuery();
		Vector<String> qv = query._tokens;

		Queue<ScoredDocument> retrieval_results = new PriorityQueue<ScoredDocument>(numResults);
		Document doc = null;
		int docid = -1;

		while ((doc = _index.nextDoc(query, docid)) != null) {
			retrieval_results.add(runquery_QL(qv, docid));
			if (retrieval_results.size() > numResults) {
				retrieval_results.poll();
			}
			docid = doc._docid;
		}
		
		
		Vector<ScoredDocument> results = new Vector<ScoredDocument>();
		ScoredDocument scoredDoc = null;
		while ((scoredDoc = retrieval_results.poll()) != null) {
			results.add(scoredDoc);
		}
		Collections.sort(results, Collections.reverseOrder());
		return results;
	}

	private ScoredDocument runquery_QL(Vector<String> query, int did) {
		DocumentIndexed doc = (DocumentIndexed) _index.getDoc(did);
		double score = 0;
		double lambda = 0.5;
		for (String q : query) {
			double docTermFreq = _indexer.documentTermFrequency(q, doc.getUrl());
			double totalWords_doc = doc.getTotalWords();
			double corpusTermFreq = _index.corpusTermFrequency(q);
			double totalWords_corpus = _index.totalTermFrequency();
			double temp = 0.0;

			if (totalWords_doc != 0) {
				temp += (1 - lambda) * ((1.0 * docTermFreq) / (1.0 * totalWords_doc));
			}
			if (totalWords_corpus != 0) {
				temp += (lambda) * ((1.0 * corpusTermFreq) / (1.0 * totalWords_corpus));
			}

			score += (Math.log(temp) / Math.log(2));
		}

		return new ScoredDocument(doc, Math.pow(2, score));
	}

}
