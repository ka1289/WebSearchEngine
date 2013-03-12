package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends Indexer {
	private HashMap<String, WordAttribute_compressed>[] mapOfMaps;
	private Map<String, WordAttribute_compressed> wordMap = new HashMap<String, WordAttribute_compressed>();
	private Map<Integer, DocumentIndexed> docMap = new HashMap<Integer, DocumentIndexed>();

	public IndexerInvertedCompressed(Options options) {
		super(options);
		System.out.println("Using Indexer: " + this.getClass().getSimpleName());
	}

	@Override
	public void constructIndex() throws IOException {
		File corpusDir = new File(_options._corpusPrefix);
		File[] listOfFiles = corpusDir.listFiles();
		int noOfFiles = listOfFiles.length;

		int i = 0;
		int index = 1;
		initializeMap();
		
		for (File eachFile : listOfFiles) {
			if (i >= noOfFiles / 5) {
//				serialize();
				mapOfMaps = null;
				i = 0;
				initializeMap();
			}
			analyse(eachFile, index);
			index++;
			i++;
		}
		
	}

	private void analyse(File eachFile, int index) {
		
	}

	@SuppressWarnings("unchecked")
	private void initializeMap() {
		mapOfMaps = (HashMap<String, WordAttribute_compressed>[]) new HashMap[199];
		for (int j = 0; j < 199; j++) {
			mapOfMaps[j] = new HashMap<String, WordAttribute_compressed>();
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
		QueryPhrase queryPhrase = new QueryPhrase(query._query);
		queryPhrase.processQuery();
		
		List<String> phrases = new ArrayList<String>();
		StringBuilder tokens = new StringBuilder();
		for(String strTemp : queryPhrase._tokens) {
		//Checking of the string is a phrase or not
			if(strTemp.split(" ").length > 1) {
				phrases.add(strTemp);
			}	
			else {
				tokens.append(strTemp);
			}
		}
		
		//I get the next document to be checked for phrase which contains all the other
		//non phrase tokens
		//Run a Loop here-----
		DocumentIndexed documentToBeCheckedForPhrases = nextDocToken(new Query(tokens.toString()), docid);
		
		return null;
	}
	
	
	

	private DocumentIndexed nextDocToken(Query query, int docid) {
		query.processQuery();
		
	// First find out the smallest list among the list of all the words
		String smallestListWord = findWordWithSmallestList(query);
		
		
	  return null;
  }

	private String findWordWithSmallestList(Query query) {
		int minListLength = Integer.MAX_VALUE;
		String smallestListWord = "";
		for(String strTemp : query._tokens) {
			WordAttribute_compressed currentWordAttribute_compressed = wordMap.get(strTemp);
			int mapSize = currentWordAttribute_compressed.getList().size();
			
			
			
			
			
		}
		
		
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

	/**
	 * @CS2580: Implement this for bonus points.
	 */
	@Override
	public int documentTermFrequency(String term, String url) {
		// TODO
		return 0;
	}
}
