import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerCorrelation extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  
  private HashMap<Integer, Double> rootSquaredAdjustedMap = new HashMap<Integer, Double>();
  
  private DoubleWritable similarityWritable = new DoubleWritable();
  private Text combinedKeyText = new Text();
  
  
  
  @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
        
        Path[] localPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (localPaths.length == 0) {
          throw new FileNotFoundException("Distributed cache file not found.");
        }
        BufferedReader in = null;
        for (Path localPath : localPaths) {
          File localFile = new File(localPath.toString());
          try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(localFile)));
            String line;
            String[] entries;
            while ((line = in.readLine()) != null) {
              entries = line.split("\\t");
              if (entries.length == 2) {
                rootSquaredAdjustedMap.put(Integer.parseInt(entries[0]),Double.parseDouble(entries[1]));
              }
            }
          } finally {
              IOUtils.closeStream(in);
            }
        }
      
      
    }
  
  
  @Override
  public void reduce(Text itemIds, Iterable<DoubleWritable> adjustedRatings, Context context)
          throws IOException, InterruptedException {
    
    Double adjustedSum = 0.0;
    
    //get the ids as string then parse as integers and get the norms for each id from the map 
    String[] items = itemIds.toString().split(",");
    Double norm_i =  rootSquaredAdjustedMap.get(Integer.parseInt(items[0]));
    Double norm_j =  rootSquaredAdjustedMap.get(Integer.parseInt(items[1]));
    
    for(DoubleWritable adjustedRating : adjustedRatings){
      // Sum the correlations
      adjustedSum += adjustedRating.get();
    }
    
    Double similarity = adjustedSum /(norm_i * norm_j);
    // (item_ids, adjustedSumWritable) for that id
    // item_ids will again be like (item_id1, item_id2)
    similarityWritable.set(similarity);
    context.write(itemIds, similarityWritable); 
  }
  

}
