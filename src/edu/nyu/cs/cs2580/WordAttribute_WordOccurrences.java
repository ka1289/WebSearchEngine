package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WordAttribute_WordOccurrences {

	private Map<Integer, ArrayList<Integer>> list = new HashMap<Integer, ArrayList<Integer>>();
	private long freq;

	public Map<Integer, ArrayList<Integer>> getList() {
		return list;
	}

	public void setList(HashMap<Integer, ArrayList<Integer>> list) {
		this.list = list;
	}

	public long getFreq() {
		return freq;
	}

	public void setFreq(long freq) {
		this.freq = freq;
	}
}
