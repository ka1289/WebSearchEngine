package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.List;

public class WordAttribute {

	private List<Integer> list = new ArrayList<Integer>();
	private int freq;

	public List<Integer> getList() {
		return list;
	}

	public void setList(List<Integer> list) {
		this.list = list;
	}

	public int getFreq() {
		return freq;
	}

	public void setFreq(int freq) {
		this.freq = freq;
	}

}
