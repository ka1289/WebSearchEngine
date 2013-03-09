package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class AppendingObjectInputStream extends ObjectInputStream {

	public AppendingObjectInputStream(InputStream out) throws IOException {
		super(out);
	}

	@Override
	protected void readStreamHeader() throws IOException {
	}

}