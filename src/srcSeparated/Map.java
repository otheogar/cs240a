

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



public class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable > {
		
                private IntWritable id = new IntWritable();
                private DoubleWritable adj_rating = new DoubleWritable();
		
        @Override
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
                                
                                String line = value.toString();
                                
                               
                                String[] keyval = line.split("\\t");
                                
                                //uid.set(Integer.parseInt(entries[0]));//don't need the uid
                                if(keyval.length != 2){
                                                return;
                                }
                                String[] entries = keyval[1].split(";"); //items
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
                                double avg = (double)(sum)/(double)(n);
                                
                                Iterator<Entry<Integer,Integer>> it = items.entrySet().iterator();
                                
                                while (it.hasNext()) {
                                                Entry<Integer,Integer> en = it.next();
                                                id.set(en.getKey());
                                                double adjrating = en.getValue()-avg;
                                                double adjrating_squared = adjrating * adjrating;
                                                adj_rating.set(adjrating_squared);
                                                context.write(id,adj_rating);
                                }                
                                
                                
                                
			
		}
}

