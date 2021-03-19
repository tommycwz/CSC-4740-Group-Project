package tigerGambit;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.text.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChessReducer extends Reducer<Text, Text, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException{
	  
			  
	  int count = 0;
	  
	  
	  for (Text value : values) {
		  count++;
	  }
	  
	  context.write(key, new IntWritable(count));
  }
  
}