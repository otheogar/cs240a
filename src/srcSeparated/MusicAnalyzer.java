

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
		//xString cacheInputPath = args[0];
		
		Configuration conf1 = new Configuration();
		
		if (args.length != 2) {
			System.err.println("Usage: musicanalyzer <in> <out>");
			System.exit(2);
		}
		
		Job job1 = new Job(conf1, "calculate norms");
		job1.setJarByClass(MusicAnalyzer.class);
		job1.setMapperClass(Map.class);
		
		job1.setCombinerClass(Reduce.class);
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);
		job1.setNumReduceTasks(8);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(cachePath));
		
		if(!job1.waitForCompletion(true)) {
			return 1;
		}
		
		FileSystem fs = FileSystem.get(conf1);
		Job job2 = new Job(new Configuration(), "calculate simmilarity measure");
		Configuration conf2 = job2.getConfiguration();
		DistributedCache.createSymlink(conf2);
		
		FileStatus[] fsStatus = fs.globStatus(new Path(cachePath+"/part*"));
		int i=0;
		for (FileStatus f: fsStatus) {
			Path fp = f.getPath();
			//System.out.println("cache file: " + fp.toString());
			String symlink = fp.toUri().toString() + "#myfile"+i;
			//System.out.println("cache file symlink: " + symlink);
			DistributedCache.addCacheFile(new URI(symlink),conf2);
			i++;
		}
		
		
		job2.setJarByClass(MusicAnalyzer.class);
		job2.setMapperClass(MapCorrelation.class);
		
		job2.setCombinerClass(CombinerCorrelation.class);
		job2.setReducerClass(ReducerCorrelation.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setNumReduceTasks(8);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/corr"));
		
		Job job3 = new Job(new Configuration(), "find top k similar items");
		
		
		Configuration conf3 = job3.getConfiguration();
		/*
	  DistributedCache.createSymlink(conf3);
	  FileStatus[] fsUserStatus = fs.globStatus(new Path(cacheInputPath+"/a"));
	  String userFileSymLink = fsUserStatus[0].getPath().toUri().toString()+"#userFile";
	  //Path userInfoFilePath = new Path(cacheInputPath+"/a");
    //String userFileSymLink = userInfoFilePath.toUri().toString()+"#userFile";
    DistributedCache.addCacheFile(new URI(userFileSymLink),conf3);
    */
    
		job3.setJarByClass(MusicAnalyzer.class);
		job3.setMapperClass(MapIdentity.class);
		
		//job2.setCombinerClass(Reduce.class);
		job3.setReducerClass(ReducerTopK.class);
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(TopKRecord.class);
		job3.setNumReduceTasks(8);
		FileInputFormat.addInputPath(job3, new Path(args[1]+"/corr"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/topk"));
		
		return ((job2.waitForCompletion(true)) && (job3.waitForCompletion(true)))? 0 : 1;		
		
		
		
	}

	public static void main(String[] args) throws Exception {
		
		
		int res = ToolRunner.run(new Configuration(), new MusicAnalyzer(), args);
		System.exit(res);
	}

}
