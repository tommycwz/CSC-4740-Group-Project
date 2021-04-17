package tigerGambit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessAnalysis {

  public static void main(String[] args) throws Exception {

	  if (args.length != 2) {
	      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Project: Chess Analysis");
	    job.setJarByClass(ChessAnalysis.class);

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.setMapperClass(ChessMapper.class);
	    job.setReducerClass(ChessReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setPartitionerClass(ChessPartitioner.class);
	    job.setNumReduceTasks(7);
	    
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
  }
}
