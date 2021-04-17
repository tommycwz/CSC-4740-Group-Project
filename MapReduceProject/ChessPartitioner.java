package tigerGambit;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class ChessPartitioner<K2, V2> extends Partitioner<Text, Text> implements
    Configurable {

	/*
	 *  Create HashMap to hold the 7 Elo ranges. There will be a partition for each range.
	 */
  private Configuration configuration;
  HashMap<String, Integer> EloRanges = new HashMap<String, Integer>();

  @Override
  public void setConf(Configuration configuration) {
	 this.configuration = configuration;
	 
	 /* Set the elo ranges for the hashmap. */
	 	EloRanges.put("0 to 1200", 0);
		EloRanges.put("1201 to 1500", 1);
		EloRanges.put("1501 to 1800" , 2);
		EloRanges.put("1801 to 2000", 3);
		EloRanges.put("2001 to 2200", 4);
		EloRanges.put("2201 to 2400" , 5);
		EloRanges.put("2401 to Beyond" , 6);

  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  /*
   *  use the key to grab partitioner number from HashMap.
   */
  public int getPartition(Text key, Text value, int numReduceTasks) {
	  
	  String line = value.toString();
	  String[] spt = line.split("=");
	  
	 if (Integer.parseInt(spt[1]) >= 0 && Integer.parseInt(spt[1]) <= 1200){
		 return 0 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[1]) >= 1201 && Integer.parseInt(spt[1]) <= 1500){
		 return 1 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[1]) >= 1501 && Integer.parseInt(spt[1]) <= 1800){
		 return 2 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[1]) >= 1801 && Integer.parseInt(spt[1]) <= 2000){
		 return 3 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[1]) >= 2001 && Integer.parseInt(spt[1]) <= 2200){
		 return 4 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[1]) >= 2201 && Integer.parseInt(spt[1]) <= 2400){
		 return 5 % numReduceTasks;
	 }
	 else {
		 return 6 % numReduceTasks;
	 }
	 
  }
}