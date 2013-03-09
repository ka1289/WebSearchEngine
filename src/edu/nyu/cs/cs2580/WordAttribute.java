package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.List;

public class WordAttribute {

	private List<Integer> list = new ArrayList<Integer>();
	private long freq;

	public List<Integer> getList() {
		return list;
	}

	public void setList(List<Integer> list) {
		this.list = list;
	}

	public long getFreq() {
		return freq;
	}

	public void setFreq(long freq) {
		this.freq = freq;
	}

}
