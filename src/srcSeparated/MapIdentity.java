

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import UIdAdjustedRating

import java.io.IOException;
import java.util.regex.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;



public class MapIdentity extends Mapper<LongWritable, Text, IntWritable, TopKRecord> {
  
  
  private TopKRecord similarityRecord = new TopKRecord();
  private IntWritable idiWritable = new IntWritable(0);
		
        @Override
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
                                
                                String line = value.toString();
                                
                               
                                String[] keyval = line.split("\\t");
                                idiWritable.set(Integer.parseInt(keyval[0]));
                                String[] s = keyval[1].split(",");
                                similarityRecord.itemId = Integer.parseInt(s[0].replace("(",""));
                                similarityRecord.similarityMeasure = Double.parseDouble(s[1].replace(")",""));
                                context.write(idiWritable,similarityRecord);
			
		}
}

