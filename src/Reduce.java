import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
  
  private HashMap<Integer, Double> rootAdjustedSquaredMap = new HashMap<Integer, Double>();
  
  @Override
  public void reduce(IntWritable itemId, Iterable<DoubleWritable> squaredAdjustedRatings, Context context)
          throws IOException, InterruptedException {
    
    Double squaredAdjustedSum = 0.0;
    DoubleWritable rootSquaredAdjustedSum = new DoubleWritable();
    
    for(DoubleWritable squaredAdjustedRating : squaredAdjustedRatings){
      // Sum the adjusted ratings
      squaredAdjustedSum += squaredAdjustedRating.get();
    }
    
    // Take the square root of the squared sum
    rootSquaredAdjustedSum.set(Math.sqrt(squaredAdjustedSum));
    // (item_id, rootSquaredAsjustedSum) for that id
    context.write(itemId, rootSquaredAdjustedSum);
    rootAdjustedSquaredMap.put(itemId.get(), rootSquaredAdjustedSum.get());
  }
  
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    // add the file to the Distributed cache here 
    
    //DistributedCache.addCacheArchive(uri, conf);
  }

}
