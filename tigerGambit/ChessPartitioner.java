package tigerGambit;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class ChessPartitioner<K2, V2> extends Partitioner<Text, Text> implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> color = new HashMap<String, Integer>();

  @Override
  public void setConf(Configuration configuration) {
	 this.configuration = configuration;
	 
	  color.put("white", 0);
	  color.put("black", 1);
	  color.put("draw" , 2);

  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  public int getPartition(Text key, Text value, int numReduceTasks) {
     return color.get(key.toString());
  }
}