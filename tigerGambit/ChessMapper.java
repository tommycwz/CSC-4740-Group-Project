package tigerGambit;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChessMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  try{
		  String line = value.toString();
		  String[] spt = line.split("/");
		  
		  String rule = spt[0];
		  String result = spt[1];
		  int whiteElo = Integer.parseInt(spt[2]);
		  int blackElo = Integer.parseInt(spt[3]);
		  String opening = spt[4];
		  String winner = "";
		  int winnerElo = 0;
		  
		  if(result.equalsIgnoreCase("0-1")){
			  winner = "black";
			  winnerElo = blackElo;
		  }
		  else if(result.equalsIgnoreCase("1-0")){
			  winner = "white";
			  winnerElo = whiteElo;
		  }
		  else if(result.equalsIgnoreCase("1/2-1/2")){
			  winner = "draw";
			  winnerElo = Math.round(whiteElo + blackElo / 2);
		  }
		  else{
			  System.err.println("Error: Invalid Result on winner");
		  }
		  
		  String val = rule + "/" + winnerElo + "/" + opening;
		  
		  if(!winner.isEmpty() && !val.isEmpty()){
			  context.write(new Text(winner), new Text(val));
		  }
	      
	  }
	  
	  catch(Exception e){
		  System.err.println("Error");
	  }
	  

	  
  }
}