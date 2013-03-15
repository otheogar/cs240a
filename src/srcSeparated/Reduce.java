import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
  
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
    
  }

}
