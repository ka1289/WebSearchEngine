package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer {
	private Map<String, HashMap<String, WordAttribute>> mapOfMaps;
	private Map<Integer, DocumentIndexed> docMap = new HashMap<Integer, DocumentIndexed>();
	private Map<String, WordAttribute> wordMap = new HashMap<String, WordAttribute>();

	public IndexerInvertedDoconly(Options options) {
		super(options);
		System.out.println("Using Indexer: " + this.getClass().getSimpleName());
	}

	private List<String> tokenize(TokenStream stream) throws IOException {
		List<String> words = new ArrayList<String>();

		CharTermAttribute attr = stream.addAttribute(CharTermAttribute.class);
		while (stream.incrementToken()) {
			words.add(attr.toString());
		}

		stream.end();
		stream.close();
		return words;
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
				serialize();
				mapOfMaps = null;
				i = 0;
				initializeMap();
			}
			analyse(eachFile, index);
			index++;
			i++;
		}
		
		try {
			merge();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void merge() throws FileNotFoundException, IOException, ClassNotFoundException {
		File indexDir = new File(_options._indexPrefix);
		File[] indexedFiles = indexDir.listFiles();
		
		for(File file : indexedFiles) {
			AppendingObjectOutputStream oos = new AppendingObjectOutputStream(new FileOutputStream(file.getAbsoluteFile(), true));
			oos.writeObject(new NullObject());
			oos.close();
		}
		indexedFiles = indexDir.listFiles();
		for(File file : indexedFiles) {
			AppendingObjectInputStream ois = new AppendingObjectInputStream(new FileInputStream(file.getAbsoluteFile()));
			Object o;
			while (! ( ( o = ois.readObject()) instanceof NullObject) ) {
				Map<String, WordAttribute> map = (Map<String, WordAttribute>) o;
				for (Map.Entry<String, WordAttribute> entry : map.entrySet()) {
					String key = entry.getKey();
					WordAttribute wa = entry.getValue();
					if (wordMap.containsKey(key)) {
						WordAttribute wa1 = wordMap.get(key);
						wa1.setFreq(wa1.getFreq() + wa.getFreq());
						wa1.getList().addAll(wa.getList());
					} else {
						wordMap.put(key, wa);
					}
				}
			}
			
			String s = file.getName().split("_")[0];
			AppendingObjectOutputStream aoos = new AppendingObjectOutputStream(new FileOutputStream(_options._indexPrefix + "/" + s + ".ser", true));
			aoos.writeObject(wordMap);
			aoos.close();
			wordMap.clear();
			ois.close();
		}
	}

	private void serialize() throws IOException {
		for (String firstLetter : mapOfMaps.keySet()) {
			StringBuilder file = new StringBuilder(_options._indexPrefix).append("/").append(firstLetter)
					.append("_tmp.csv");
			AppendingObjectOutputStream oos = new AppendingObjectOutputStream(new FileOutputStream(file.toString(), true));
			oos.writeObject(mapOfMaps.get(firstLetter));
			oos.close();
		}
	}

	private void analyse(File eachFile, int index) throws IOException {
		DocumentIndexed docIndexed = new DocumentIndexed(index);
		docIndexed.setTitle(eachFile.getName());
		docIndexed.setUrl(eachFile.getPath());

		docMap.put(index, docIndexed);

		String newFile = Parser.parse(eachFile);
		Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_30, new HashSet<String>());
		List<String> words = tokenize(analyzer.tokenStream("", new StringReader(newFile)));
		for (String word : words) {
			String stemmed = Stemmer.execute(word).trim();
			if (stemmed.matches("[A-Za-z0-9\\p{Punct}\\s]+")) {
				String first = stemmed.substring(0, 1);
				HashMap<String, WordAttribute> currCharMap = mapOfMaps.get(first);
				if (currCharMap.containsKey(stemmed)) {
					WordAttribute tmpAttr = currCharMap.get(stemmed);
					List<Integer> tmpList = tmpAttr.getList();
					if (!tmpList.contains(index)) {
						tmpList.add(index);
					}
					long tmpFreq = tmpAttr.getFreq();
					tmpFreq++;
					tmpAttr.setFreq(tmpFreq);
				} else {
					WordAttribute tmpAttr = new WordAttribute();
					List<Integer> tmpList = new ArrayList<Integer>();
					tmpList.add(index);
					tmpAttr.setFreq(1);
					currCharMap.put(stemmed, tmpAttr);
				}
			}
		}
		analyzer.close();
	}

	private void initializeMap() {

		mapOfMaps = new HashMap<String, HashMap<String, WordAttribute>>();

		for (int j = 0; j < 26; j++) {
			HashMap<String, WordAttribute> tmp = new HashMap<String, WordAttribute>();
			mapOfMaps.put('a' + j + "", tmp);
		}

		for (int j = 0; j < 10; j++) {
			HashMap<String, WordAttribute> tmp = new HashMap<String, WordAttribute>();
			mapOfMaps.put('0' + j + "", tmp);
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
