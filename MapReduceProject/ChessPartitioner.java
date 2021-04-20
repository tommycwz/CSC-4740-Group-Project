package tigerGambit;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class ChessPartitioner<K2, V2> extends Partitioner<Text, Text> implements
    Configurable {

	
  private Configuration configuration;

  @Override
  public void setConf(Configuration configuration) {
	 this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  /*
   *  send data to proper corresponding partition
   */
  public int getPartition(Text key, Text value, int numReduceTasks) {
	  
	  String line = value.toString();
	  String[] spt = line.split("=");
	  
	 if (Integer.parseInt(spt[0]) >= 0 && Integer.parseInt(spt[0]) <= 1200){
		 return 0 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 1201 && Integer.parseInt(spt[0]) <= 1500){
		 return 1 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 1501 && Integer.parseInt(spt[0]) <= 1800){
		 return 2 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 1801 && Integer.parseInt(spt[0]) <= 2000){
		 return 3 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 2001 && Integer.parseInt(spt[0]) <= 2200){
		 return 4 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 2201 && Integer.parseInt(spt[0]) <= 2400){
		 return 5 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 2401 && Integer.parseInt(spt[0]) <= 2600){
		 return 6 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 2601 && Integer.parseInt(spt[0]) <= 2800){
		 return 7 % numReduceTasks;
	 }
	 else if(Integer.parseInt(spt[0]) >= 2801 && Integer.parseInt(spt[0]) <= 3000){
		 return 8 % numReduceTasks;
	 }
	 else {
		 return 9 % numReduceTasks;
	 }
	 
  }
}