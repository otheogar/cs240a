import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.util.*;
import java.util.Map.Entry;
import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CombinerCorrelation extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private DoubleWritable sumWritable = new DoubleWritable();

  
  @Override
  public void reduce(Text itemIds, Iterable<DoubleWritable> adjustedRatings, Context context)
          throws IOException, InterruptedException {
    
    Double adjustedSum = 0.0;
    
    for(DoubleWritable adjustedRating : adjustedRatings){
      // Sum the correlations
      adjustedSum += adjustedRating.get();
    }
    sumWritable.set(adjustedSum);
    context.write(itemIds, sumWritable);
    
  }
  

}
