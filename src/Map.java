

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import UIdAdjustedRating



public class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable > {
		private IntWritable id = new IntWritable();
    private DoubleWritable adjRatingWritable = new DoubleWritable();
		
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
            Double avg = (double)sum/(double)n;
            
            Iterator<Entry<Integer,Integer>> it = items.entrySet().iterator();
            
            while (it.hasNext()) {
                            Entry<Integer,Integer> en = it.next();
                            id.set(en.getKey());
                            Double adjRating = en.getValue()-avg;
                            Double adjratingSquared = adjRating * adjRating;
                            adjRatingWritable.set(adjratingSquared);
                            // item_id, adjusted rating for it
                            context.write(id,adjRatingWritable);
            }                                         
		}
}

