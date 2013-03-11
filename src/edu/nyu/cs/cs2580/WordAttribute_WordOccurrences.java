package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class WordAttribute_WordOccurrences {
	// Maps the docid to the positions at which the word occurred
	private LinkedHashMap<Integer, ArrayList<Integer>> list = new LinkedHashMap<Integer, ArrayList<Integer>>();
	private long freq;

	public LinkedHashMap<Integer, ArrayList<Integer>> getList() {
		return list;
	}

	public void setList(LinkedHashMap<Integer, ArrayList<Integer>> list) {
		this.list = list;
	}

	public long getFreq() {
		return freq;
	}

	public void setFreq(long freq) {
		this.freq = freq;
	}
}
