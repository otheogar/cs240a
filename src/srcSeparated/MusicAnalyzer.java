

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.regex.*;
import java.util.*;
import java.io.*;
import java.net.URI;


public class MusicAnalyzer extends Configured implements Tool{
	
	

	public int run(String[] args) throws Exception {
		
		String cachePath = args[1]+"/cache";
		
		Configuration conf1 = new Configuration();
		
		if (args.length != 2) {
			System.err.println("Usage: musicanalyzer <in> <out>");
			System.exit(2);
		}
		
		Job job1 = new Job(conf1, "analyze music file");
		job1.setJarByClass(MusicAnalyzer.class);
		job1.setMapperClass(Map.class);
		
		//job1.setCombinerClass(Reduce.class);
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);
		//job1.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(cachePath));
		
		if(!job1.waitForCompletion(true)) {
			return 1;
		}
		
		FileSystem fs = FileSystem.get(conf1);
		Job job2 = new Job(new Configuration(), "analyze music file2");
		Configuration conf2 = job2.getConfiguration();
		DistributedCache.createSymlink(conf2);
		
		FileStatus[] fsStatus = fs.globStatus(new Path(cachePath+"/part*"));
		int i=0;
		for (FileStatus f: fsStatus) {
			Path fp = f.getPath();
			System.out.println("cache file: " + fp.toString());
			String symlink = fp.toUri().toString() + "#myfile"+i;
			System.out.println("cache file symlink: " + symlink);
			DistributedCache.addCacheFile(new URI(symlink),conf2);
			i++;
		}
		
		
		job2.setJarByClass(MusicAnalyzer.class);
		job2.setMapperClass(MapCorrelation.class);
		
		//job2.setCombinerClass(Reduce.class);
		job2.setReducerClass(ReducerCorrelation.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		//job2.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/corr"));
		
		return (job2.waitForCompletion(true)) ? 0 : 1;		
		
		
		
	}

	public static void main(String[] args) throws Exception {
		
		
		int res = ToolRunner.run(new Configuration(), new MusicAnalyzer(), args);
		System.exit(res);
	}

}
