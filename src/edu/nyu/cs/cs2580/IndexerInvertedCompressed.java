package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
	private Map<String, WordAttribute_WordOccurrences> wordMapUncompressed = new HashMap<String, WordAttribute_WordOccurrences>();

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
				// serialize();
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

	private ArrayList<Byte> encode(int value) {
		ArrayList<Byte> array = new ArrayList<Byte>();

		while (value > 0) {
			int num = (int) Math.pow(2, 7);
			num = num - 1;
			byte b = (byte) (value & num);
			array.add(b);
			value = value >> 7;
		}
		return array;
	}
	
	private int decode(ArrayList<Byte> array) {
		int out = 0;
		
		for(int j=0;j<array.size();j++) {
			int tmp = (int)Math.pow(128, j);
			out += tmp * array.get(j);
		}
		return out;
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
		wordMapUncompressed = new HashMap<String, WordAttribute_WordOccurrences>();
		
		File indexDir = new File(_options._indexPrefix);
		File[] indexedFiles = indexDir.listFiles();
		int noFilesLoaded = 0;
		for (File file : indexedFiles) {
			if (file.getName().equals(".DS_Store")) { continue;				
			}
			
			if (file.getName().equals("doc_map.csv")) {
				loadDocMap(file);
				continue;
			}
			
			if (noFilesLoaded < 50 && !file.getName().equals("doc_map.csv") && !file.getName().equals(".DS_Store")) {				
				loadFile(file);
				noFilesLoaded++;
			}			
		}
	}

	private void loadFile(File file) throws IOException {	   
		BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
		String line = "";
		while (((line = br.readLine()) != null)) {
			String[] eachLine = line.split("\t");
			String word = eachLine[0];
			WordAttribute_WordOccurrences wordAttribute_WordOccurrences = new WordAttribute_WordOccurrences();
		// get the frequency for the words
			wordAttribute_WordOccurrences.setFreq(toInteger(eachLine[eachLine.length - 1]));
			LinkedHashMap<Integer, ArrayList<Integer>> currMap = new LinkedHashMap<Integer, ArrayList<Integer>>();
			
			int i = 1;
			while (i < eachLine.length - 1) {
				int did = (int) toInteger(eachLine[i]);
				i++;
				int frequencyInDoc = Integer.parseInt(eachLine[i]);
				i++;
				int k = 0;
				int prev = 0;
				ArrayList<Integer> list = new ArrayList<Integer>();
				while (k < frequencyInDoc) {
					int temp = (int)toInteger(eachLine[i]);
					list.add(temp + prev);
					prev = temp;
					k++;
					i++;
				}
				currMap.put(did, list);
			}
			wordAttribute_WordOccurrences.setList(currMap);
			wordMapUncompressed.put(word, wordAttribute_WordOccurrences);
		}
		br.close();
  }
	
	private long toInteger(String input) {
		ArrayList<Byte> byteList = getList(input.getBytes());
		long output = decode(byteList);
		return output;
	}

	private void loadDocMap(File file) throws NumberFormatException, IOException {  
	  docMap = new HashMap<Integer, DocumentIndexed>();
	  
	  BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
	  String temp = "";
	  int count = 0;
	  while ((temp = br.readLine()) != null) {
	  	String[] eachLine = temp.split("\t");	  	
	  	 ArrayList<Byte> byteList = getList(eachLine[0].getBytes());
	  	 int did = decode(byteList);
	  	if (count > 2000) {
				break;
			}
	  	DocumentIndexed currentDoc = new DocumentIndexed(did);
	  	String title = eachLine[1];
			String url = eachLine[2];
			long totalWords = Integer.parseInt(eachLine[eachLine.length - 1]);
			
			currentDoc.setTitle(title);
			currentDoc.setUrl(url);
			currentDoc.setTotalWords(totalWords);  	
	  }
  }
	
	private ArrayList<Byte> getList(byte[] bs) {
		ArrayList<Byte> output = new ArrayList<Byte>();
		for(Byte b : bs) {
			output.add(b);
		}
		return output;
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
