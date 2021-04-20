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
		  
		  /*
		   * Read input into "line" variable. Then split the line by the "=" delimiter 
		   * and store it in string array "spt".
		   */
		  String line = value.toString();
		  String[] spt = line.split("=");
		  
		  /*
		   * result stores game result
		   */
		  String result = spt[1];
		  
		  /*
		   * whiteElo stores white's rank
		   * blackElo stores black's rank
		   */
		  int whiteElo = Integer.parseInt(spt[2]);
		  int blackElo = Integer.parseInt(spt[3]);
		  
		  /*
		   * opening stores the opening play of the game
		   */
		  String opening = spt[4];
		  
		  /*
		   * create variables to hold the winner's elo and color
		   */
		  String winner = "";
		  int winnerElo = 0;
		  
		  /* Set winner string and elo to black if they won */
		  if(result.equalsIgnoreCase("0-1")){
			  winner = "black";
			  winnerElo = blackElo;
		  }
		  /* Else if set winner string and elo to white if they won */
		  else if(result.equalsIgnoreCase("1-0")){
			  winner = "white";
			  winnerElo = whiteElo;
		  }
		  /* Else if set winner string to draw and set elo to the average of both players elo */
		  else if(result.equalsIgnoreCase("1/2-1/2")){
			  winner = "draw";
			  winnerElo = Math.round((whiteElo + blackElo) / 2);
		  }
		  /* Otherwise output an error due to format */
		  else{
			  System.err.println("Error: Invalid Result on winner");
		  }
		  
		  /*
		   * Create a new string to hold winner's elo, and opening using
		   *    = as the delimiter.
		   */
		  String val = winnerElo + "=" + opening;
		  
		  /*
		   *  if winner and value isnt empty, create a new context to pass to reducer
		   */
		  if(!winner.isEmpty() && !val.isEmpty()){
			  
			  context.write(new Text(winner), new Text(val));
		  }
	      
	  }
	  /* catch error */
	  catch(Exception e){
		  System.err.println("Error");
	  }
  }
}