import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.lang.StringBuffer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerTopK extends Reducer<IntWritable, TopKRecord, IntWritable, Text> {
  private static Integer TOPK = 30; //find most similar 30 items
  private TreeSet<TopKRecord> recommendedItemBucket = new TreeSet<TopKRecord>();
  private Text topKList = new Text();
  
  @Override
  public void reduce(IntWritable itemId, Iterable<TopKRecord> similarities, Context context)
          throws IOException, InterruptedException {
    //output the list of pairs itemid,similarity_measure fro the top k most similar items for this item
     
    for(TopKRecord k: similarities) {
      this.recommendedItemBucket.add(k);
        // Currently we are not testing whether the items we are recommending 
        // are already present in his list of elements
        if(this.recommendedItemBucket.size() >= TOPK){
          this.recommendedItemBucket.pollFirst();
        } 
     }
     
    StringBuffer sb = new StringBuffer();
    for (TopKRecord topk: recommendedItemBucket) {
      if(sb.length() != 0) {
        sb.append(";");
      }
      sb.append(topk.itemId.toString()+","+topk.similarityMeasure.toString());
    }
    
    topKList.set(sb.toString());
    context.write(itemId, topKList);
  
  }

}
