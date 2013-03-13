

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.regex.*;
import java.util.*;


public class MusicAnalyzer extends Configured implements Tool{
	
	

	public int run(String[] args) throws Exception {
		
		
		Configuration conf1 = new Configuration();
		/*Configuration conf2 = new Configuration();
		Configuration confURL = new Configuration();
		Configuration confURL2 = new Configuration();
		*/
		if (args.length != 2) {
			System.err.println("Usage: musicanalyzer <in> <out>");
			System.exit(2);
		}
		
		Job job1 = new Job(conf1, "analyze music file");
		job1.setJarByClass(MusicAnalyzer.class);
		job1.setMapperClass(Map.class);
		
		//job1.setCombinerClass(Reduce.class);
		//job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IdRatingsArrayWritable.class);
		//job1.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		
		return job1.waitForCompletion(true) ? 0 : 1;		
		
		
		
	}

	public static void main(String[] args) throws Exception {
		
		
		int res = ToolRunner.run(new Configuration(), new MusicAnalyzer(), args);
		System.exit(res);
	}

}
