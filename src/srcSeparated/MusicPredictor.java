

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


public class MusicPredictor extends Configured implements Tool{
	
	

	public int run(String[] args) throws Exception {
		
		String cachePath = "cache";
		//String cacheInputPath = args[0];
		
		if (args.length != 2) {
			System.err.println("Usage: musicanalyzer <in> <out>");
			System.exit(2);
		}
		
		Job job1 = new Job(new Configuration(), "predict ratings");
		Configuration conf = job1.getConfiguration();
		DistributedCache.createSymlink(conf);
		
		String ratingsString = cachePath+"/testRatings";
		Path testRatingsFile = new Path(ratingsString);
		InputStream in = new BufferedInputStream(new FileInputStream("/home/cs240a-ucsb-34/music/testpreprocess_test.txt"));
		FileSystem fs = FileSystem.get(URI.create(ratingsString), conf);
		OutputStream out = fs.create(testRatingsFile, new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});    
		IOUtils.copyBytes(in, out, 4096, true);

		String symlink = testRatingsFile.toUri().toString() + "#testRatingsFile";
		DistributedCache.addCacheFile(new URI(symlink),conf);
		
		String simliarItemsString = cachePath + "/similarItems";
		Path similarItemsFile = new Path(simliarItemsString);
		in = new BufferedInputStream(new FileInputStream("/home/cs240a-ucsb-34/music/similarItems.txt"));
		fs = FileSystem.get(URI.create(simliarItemsString), conf);
		out = fs.create(similarItemsFile, new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});    
		IOUtils.copyBytes(in, out, 4096, true);

		symlink = similarItemsFile.toUri().toString() + "#similarItemsFile";
		DistributedCache.addCacheFile(new URI(symlink),conf);
		
		
		job1.setJarByClass(MusicAnalyzer.class);
		job1.setMapperClass(MapperPredict.class);
		
		//job1.setCombinerClass(Reduce.class);
		//job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		//job1.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		return job1.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		
		
		int res = ToolRunner.run(new Configuration(), new MusicPredictor(), args);
		System.exit(res);
	}

}
