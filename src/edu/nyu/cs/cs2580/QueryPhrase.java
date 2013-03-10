package edu.nyu.cs.cs2580;

import java.util.Scanner;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 * ["new york city"], the presence of the phrase "new york city" must be
 * recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

  public QueryPhrase(String query) {
    super(query);
  }
  
  @Override
  public void processQuery() {
  	int index = 0;
	  int i= 0;
	  String str = _query;
//	  System.out.println(str.indexOf('"',1));
	  while((i = str.indexOf('"', index)) != -1) {
	  	int j = str.indexOf('"', i+1);
	  	// if there is an odd no of quotes then an argument should be thrown
	  	if(j == -1) {
	  		throw new IllegalArgumentException();
	  	}
	  	// Will have to inside list only if the size>0
	  	String tempString = "";
	  	if((tempString = str.substring(index, i)).length() > 0) {
	  		//This will insert the individual words split by space
	  		processQuery(tempString);
	  	}	
	  	if((tempString = str.substring(i, j+1)).length() > 0) {
	  		_tokens.add(tempString.substring(1, tempString.length()-1));
	  	}	
	  	index = j+1;
	  }
	  _tokens.add(str.substring(index, str.length()));
  }
  
  private void processQuery(String _query) {
  	if (_query == null) {
      return;
    }
    Scanner s = new Scanner(_query);
    while (s.hasNext()) {
      _tokens.add(s.next());
    }
    s.close();
  }
  
}
