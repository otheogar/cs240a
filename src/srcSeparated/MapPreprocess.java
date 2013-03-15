package srcSeparated;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.*;


public class MapPreprocess extends Mapper<LongWritable, Text, IntWritable, Text> {
		
                private Text val = new Text();
		private IntWritable uid = new IntWritable(-1);
                private int nratings = 0;
                private int counter = 0;
                private StringBuffer items = new StringBuffer();
		
                
		
        @Override
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
                                String line = value.toString();
                                String[] entries;
                                System.out.println(line);
                                if(line.contains("|")) {
                                                if (counter != nratings) { //just a sanity check
                                                                System.out.println("Error for item " + uid.toString());
                                                                System.exit(1);
                                                }
                                                entries = line.split("\\|");
                                                uid.set(Integer.parseInt(entries[0]));
                                                nratings = Integer.parseInt(entries[1]);
                                                items = new StringBuffer();
                                                counter=0;
                                                val.clear();
                                }
                                else {
                                                
                                                entries = line.split("\\s+");
                                
                                                if (entries.length != 4) {
                                                                System.out.println("Error for item " + uid.toString());
                                                                System.exit(1);
                                                                }
                                                if(items.length() > 0) {
                                                                items.append(";");
                                                }
                                                for(int i=0;i<3;i++) {
                                                                items.append(entries[i]+",");                
                                                }
                                                items.append(entries[3]);
                                                counter++;
                                                
                                                //write items for the current user and clear global variables
                                                if (counter == nratings) {
                                                                val.set(items.toString());
                                                                context.write(uid,val);         
                                                                
                                                }
                                                                
                                                                               
                                                                
                                                
                                }
			
		}
}

