

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import UIdAdjustedRating

import java.io.IOException;
import java.util.regex.*;
import java.util.*;



public class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable > {
		
                private IntWritable uid = new IntWritable();
		
        @Override
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
                                
                                String line = value.toString();
                                String[] entries;
                                System.out.println(line);
                                entries = line.split("\\t");
                                uid.set(Integer.parseInt(entries[0]));
                                entries = entries[1].split(";"); //items
                                //create new hash to hold this user's ratings
                                HashMap<Integer,Integer> items = new HashMap<Integer,Integer>();
                                int sum = 0;
                                for (String e : entries) {
                                                String[] s = e.split(",");
                                                int item_id = Integer.parseInt(s[0]);
                                                int rating = Integer.parseInt(s[1]);
                                                items.put(item_id,rating);
                                                sum += rating;
                                }
                                int n = items.size();
                                int avg = sum/n;
                                IdAdjustedRating[] items_adj = new IdAdjustedRating[n];
                                Iterator it = items.entrySet().iterator();
                                while (it.hasNext()) {
                                                ;                
                                }

			
		}
}

