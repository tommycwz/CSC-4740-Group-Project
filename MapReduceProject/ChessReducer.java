package tigerGambit;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.text.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChessReducer extends Reducer<Text, Text, Text, Text> {
	
	
	HashMap<String, Integer>Openings = new HashMap<String, Integer>();

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException{
	  
	  	
	  /* ******************** INPUT EXAMPLE *************************
	   * 
	   * key = white, black, or draw as string
	   * value = GameType/WinnerElo/Opening as one string
	   * 
	   **************************************************************/
	  
	  
	  /* Set elo, and opening to strings */
	  String opening = "";
	  
	  for (Text value : values) {
		  /*  Split values into individual strings into spt array */
		  String line = value.toString();
		  String[] spt = line.split("=");
		  opening = spt[1];
		  
		  /* if opening is not in the hashmap then add it */
		  if(!Openings.containsKey(opening)){
			  Openings.put(opening, 1);
		  }else{
			  Openings.put(opening, Openings.get(opening)+1);
		  }
	  }
	  
	  /* Get max count */
	  int max = Collections.max(Openings.values());
	  
	  /* Find the entries where the count is equal to max. 
	   * If equal to max, then write to file */
	  for (Entry<String, Integer> entry : Openings.entrySet()) {
          if (entry.getValue().equals(max)) {
        	  context.write(key, new Text(entry.getKey()));
          }
      }   
	  /* Clear hashmap */
	  Openings.clear();
  }
}


