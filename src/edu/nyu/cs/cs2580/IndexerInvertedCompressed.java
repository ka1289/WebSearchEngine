package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
public class IndexerInvertedCompressed extends Indexer {
	private HashMap<String, WordAttribute_compressed>[] mapOfMaps;
	private Map<String, WordAttribute_compressed> wordMap = new HashMap<String, WordAttribute_compressed>();
	private Map<Integer, DocumentIndexed> docMap = new HashMap<Integer, DocumentIndexed>();

	public IndexerInvertedCompressed(Options options) {
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

		serialize();
		mapOfMaps = null;

		try {
			merge();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void merge() throws ClassNotFoundException, IOException {
		File indexDir = new File(_options._indexPrefix);
		File[] indexedFiles = indexDir.listFiles();

		indexedFiles = indexDir.listFiles();
		for (File file : indexedFiles) {
			if (file.getName().equals(".DS_Store") || file.getName().equals("doc_map.csv"))
				continue;

			BufferedReader ois = new BufferedReader(new FileReader(file.getAbsoluteFile()));
			String o;
			while (((o = ois.readLine()) != null)) {
				String[] eachLine = o.split("\t");
				String key = eachLine[0];
				System.out.println("---" + key + "---");
				System.out.println(file.getName());
				if (wordMap.containsKey(key)) {
					WordAttribute_compressed wa1 = wordMap.get(key);
					int temp_freq = decode(getList(eachLine[eachLine.length - 1].getBytes()));

					wa1.setFreq(temp_freq + wa1.getFreq());

					HashMap<Integer, ArrayList<ArrayList<Byte>>> currMap = wa1.getList();
					int i = 1;
					while (i < eachLine.length - 1) {
						ArrayList<Byte> temp = getList(eachLine[i].getBytes());
						int did = decode(temp);
						i++;
						int fr = decode(getList(eachLine[i].getBytes()));
						i++;
						int k = 0;
						ArrayList<ArrayList<Byte>> list = new ArrayList<ArrayList<Byte>>();
						System.out.println("once");
						while (k < fr) {
							ArrayList<Byte> byteList = getList(eachLine[i].getBytes());
							list.add(byteList);
							k++;
							i++;
						}
						if (currMap.containsKey(did)) {
							currMap.get(did).addAll(list);
						} else {
							currMap.put(did, list);
						}
					}
				} else {
					WordAttribute_compressed wa = new WordAttribute_compressed();
					int temp_freq = decode(getList(eachLine[eachLine.length - 1].getBytes()));
					wa.setFreq(temp_freq);
					LinkedHashMap<Integer, ArrayList<ArrayList<Byte>>> currMap = new LinkedHashMap<Integer, ArrayList<ArrayList<Byte>>>();

					int i = 1;
					for(int j=1;j<eachLine.length;j++) {
						System.out.print(Arrays.toString(eachLine[j].getBytes()) + " ");
					}
					System.out.println();
					while (i < eachLine.length - 1) {
						ArrayList<Byte> temp = getList(eachLine[i].getBytes());
						int did = decode(temp);
						i++;
						int fr = decode(getList(eachLine[i].getBytes()));
						System.out.println("length " + eachLine.length);
						System.out.println("key --" + key + "-- fr " + fr);
						i++;
						int k = 0;
						ArrayList<ArrayList<Byte>> list = new ArrayList<ArrayList<Byte>>();
						while (k < fr) {
							System.out.println(i);
							ArrayList<Byte> byteList = getList(eachLine[i].getBytes());
							list.add(byteList);
							k++;
							i++;
						}

						currMap.put(did, list);
					}
					wa.setList(currMap);
					wordMap.put(key, wa);
				}
			}

			String s = file.getName().split("_")[0];

			FileOutputStream fos = new FileOutputStream(new File(s.toString()), true);
			DataOutputStream oos = new DataOutputStream(fos);

			for (String si : wordMap.keySet()) {
				oos.writeBytes(si);
				oos.writeBytes("\t");
				WordAttribute_compressed temp_attr = wordMap.get(si);
				LinkedHashMap<Integer, ArrayList<ArrayList<Byte>>> temp_map = temp_attr.getList();
				for (int docid : temp_map.keySet()) {
					ArrayList<Byte> docid_bytes = encode(docid);
					writeByte(docid_bytes, oos);
					// for (Byte b : docid_bytes) {
					// oos.writeByte(b);
					// }
					oos.writeBytes("\t");

					ArrayList<ArrayList<Byte>> temp_occr = temp_map.get(docid);
					int len = temp_occr.size();
					ArrayList<Byte> len_bytes = encode(len);
					writeByte(len_bytes, oos);
					oos.writeBytes("\t");

					for (ArrayList<Byte> eachOccr : temp_occr) {
						writeByte(eachOccr, oos);
						oos.writeBytes("\t");
					}
					
				}
				int freq = temp_attr.getFreq();
				ArrayList<Byte> freqEncoded = encode(freq);
				writeByte(freqEncoded, oos);
				oos.writeBytes("\n");
			}
			oos.close();
			ois.close();
			file.delete();
		}
		wordMap.clear();
	}

	private ArrayList<Byte> getList(byte[] bs) {
		ArrayList<Byte> output = new ArrayList<Byte>();
		for (Byte b : bs) {
			output.add(b);
		}
		return output;
	}

	private void serialize() throws IOException {
		StringBuilder builder = new StringBuilder(_options._indexPrefix).append("/").append("doc_map.csv");
		BufferedWriter aoos = new BufferedWriter(new FileWriter(builder.toString(), true));
		for (int doc : docMap.keySet()) {
			aoos.write(doc + "\t");
			DocumentIndexed docIndexed = docMap.get(doc);
			aoos.write(docIndexed.getTitle() + "\t" + docIndexed.getUrl() + "\t");
			aoos.write(docIndexed.getTotalWords() + "");
			aoos.newLine();
		}
		aoos.close();
		docMap.clear();

		for (int i = 0; i < 199; i++) {
			StringBuilder file = new StringBuilder(_options._indexPrefix).append("/").append(i).append("_tmp.csv");
			FileOutputStream fos = new FileOutputStream(new File(file.toString()), true);
			DataOutputStream oos = new DataOutputStream(fos);
			HashMap<String, WordAttribute_compressed> attr = mapOfMaps[i];
			for (String s : attr.keySet()) {
				oos.writeBytes(s);
				oos.writeBytes("\t");
				WordAttribute_compressed temp_attr = attr.get(s);
				LinkedHashMap<Integer, ArrayList<ArrayList<Byte>>> temp_map = temp_attr.getList();
				for (int docid : temp_map.keySet()) {
					ArrayList<ArrayList<Byte>> temp_occr = temp_map.get(docid);
					int len = temp_occr.size();
					
					if(len == 0)
						continue;
					
					ArrayList<Byte> docid_bytes = encode(docid);
					writeByte(docid_bytes, oos);
					// for (Byte b : docid_bytes) {
					// oos.writeByte(b);
					// }
					oos.writeBytes("\t");

					ArrayList<Byte> len_bytes = encode(len);
					writeByte(len_bytes, oos);
					oos.writeBytes("\t");

					for (ArrayList<Byte> eachOccr : temp_occr) {
						writeByte(eachOccr, oos);
						oos.writeBytes("\t");
					}
					
				}
				int freq = temp_attr.getFreq();
				ArrayList<Byte> freqEncoded = encode(freq);
				writeByte(freqEncoded, oos);
				oos.writeBytes("\n");
			}
			oos.close();
		}
	}

	private void writeByte(ArrayList<Byte> arr, DataOutputStream oos) throws IOException {
		for (Byte b : arr) {
			oos.writeByte(b);
		}
	}

	private void analyse(File eachFile, int index) throws IOException {
		DocumentIndexed docIndexed = new DocumentIndexed(index);
		docIndexed.setTitle(eachFile.getName());
		docIndexed.setUrl(eachFile.getPath());

		HashSet<String> stopWords = new HashSet<String>();
		stopWords.add("the");
		stopWords.add("and");
		stopWords.add("or");
		stopWords.add("an");
		stopWords.add("if");
		stopWords.add("but");
		stopWords.add("the");
		stopWords.add("is");
		stopWords.add("an");
		stopWords.add("he");
		stopWords.add("she");
		stopWords.add("be");
		stopWords.add("me");
		stopWords.add("has");
		stopWords.add("http");
		String newFile = Parser.parse(eachFile);
		Analyzer analyzer = new EnglishAnalyzer(Version.LUCENE_30, stopWords);
		List<String> words = tokenize(analyzer.tokenStream("", new StringReader(newFile)));
		int i = 0;
		for (String word : words) {
			String stemmed = Stemmer.stemAWord(word).trim();
			if (stemmed.matches("[A-Za-z0-9]+")) {
				int hash = Math.abs(stemmed.hashCode()) % 199;
				HashMap<String, WordAttribute_compressed> currMap = mapOfMaps[hash];

				if (!currMap.containsKey(stemmed)) {
					WordAttribute_compressed currWordAttr = new WordAttribute_compressed();
					LinkedHashMap<Integer, ArrayList<ArrayList<Byte>>> currMapMap = new LinkedHashMap<Integer, ArrayList<ArrayList<Byte>>>();
					currWordAttr.setList(currMapMap);
					currMap.put(stemmed, currWordAttr);
				}

				WordAttribute_compressed currWordAttr = currMap.get(stemmed);
				LinkedHashMap<Integer, ArrayList<ArrayList<Byte>>> currMapMap = currWordAttr.getList();

				if (currMapMap.containsKey(index)) {
					int delta = 0;
					ArrayList<ArrayList<Byte>> listOfDocid = currMapMap.get(index);
					for (ArrayList<Byte> k : listOfDocid) {
						int temp_docid = decode(k);
						delta += temp_docid;
					}
					delta = i - delta;
					ArrayList<Byte> temp_deltaDocid = encode(delta);
					listOfDocid.add(temp_deltaDocid);
					currMapMap.remove(index);
					currMapMap.put(index, listOfDocid);
				} else {
					ArrayList<Byte> temp_deltaDocid = encode(i);
					ArrayList<ArrayList<Byte>> listOfDocid = new ArrayList<ArrayList<Byte>>();
					listOfDocid.add(temp_deltaDocid);
					currMapMap.put(index, listOfDocid);
				}
				
				int freq = currWordAttr.getFreq();
				freq++;
				currWordAttr.setFreq(freq);
				i++;
			}
			
		}
		analyzer.close();
		docIndexed.setTotalWords(words.size());
		docMap.put(index, docIndexed);
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

	private int decode(List<Byte> array) {
		int out = 0;

		for (int j = 0; j < array.size(); j++) {
			int tmp = (int) Math.pow(128, j);
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

	/**
	 * @CS2580: Implement this for bonus points.
	 */
	@Override
	public int documentTermFrequency(String term, String url) {
		// TODO
		return 0;
	}
}
