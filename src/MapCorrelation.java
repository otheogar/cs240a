

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



public class MapCorrelation extends Mapper<LongWritable, Text, Text, DoubleWritable > {
		
                private Text idpair = new Text();
                private DoubleWritable corr = new DoubleWritable();
		
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
                                
                                for (Entry<Integer,Integer> entry_i : items.entrySet()) {
                                                for (Entry<Integer,Integer> entry_j : items.entrySet()) {
                                                                int key_i = entry_i.getKey();
                                                                int key_j = entry_j.getKey();
                                                                if(key_i < key_j) {
                                                                                //to use symmetry and avoid double counting
                                                                                idpair.set(""+key_i+","+key_j);
                                                                                corr.set((entry_i.getValue()-avg)*(entry_j.getValue()-avg));
                                                                                context.write(idpair,corr);
                                                                }
                                                }
                                }
	
		}
}

