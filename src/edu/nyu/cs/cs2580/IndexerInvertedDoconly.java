package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer {

	public IndexerInvertedDoconly(Options options) {
		super(options);
		System.out.println("Using Indexer: " + this.getClass().getSimpleName());
	}

	@Override
	public void constructIndex() throws IOException {
		File corpusDir = new File(_options._corpusPrefix);
		File indexDir = new File(_options._indexPrefix);
		File[] listOfFiles = corpusDir.listFiles();
		int noOfFiles = listOfFiles.length;

		int i = 0;

		for (File eachFile : listOfFiles) {
			i = 0;
			
			while (i < noOfFiles / 5) {
				String newFile = Parser.parse(eachFile);
				
				i++;
			}
		}
	}

	@Override
	public void loadIndex() throws IOException, ClassNotFoundException {
	}

	@Override
	public Document getDoc(int docid) {
		return null;
	}

	/**
	 * In HW2, you should be using {@link DocumentIndexed}
	 */
	@Override
	public Document nextDoc(Query query, int docid) {
		return null;
	}

	@Override
	public int corpusDocFrequencyByTerm(String term) {
		return 0;
	}

	@Override
	public int corpusTermFrequency(String term) {
		return 0;
	}

	@Override
	public int documentTermFrequency(String term, String url) {
		SearchEngine.Check(false, "Not implemented!");
		return 0;
	}
}
