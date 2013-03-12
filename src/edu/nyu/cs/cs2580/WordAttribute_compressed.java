package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class WordAttribute_compressed {

	private LinkedHashMap<Integer, ArrayList<Byte>> list = new LinkedHashMap<Integer, ArrayList<Byte>>();
	private long freq;

	public LinkedHashMap<Integer, ArrayList<Byte>> getList() {
		return list;
	}

	public void setList(LinkedHashMap<Integer, ArrayList<Byte>> list) {
		this.list = list;
	}

	public long getFreq() {
		return freq;
	}

	public void setFreq(long freq) {
		this.freq = freq;
	}
}
