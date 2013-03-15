import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerCombined extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  
  private static String DELIMITER = ",";
  private HashMap<Integer, Double> rootSquaredAdjustedMap = new HashMap<Integer, Double>();
  private HashMap<String, Double> adjustedSumsMap = new HashMap<String, Double>();
  
  private HashMap<String, Double> similarityMap = new HashMap<String, Double>();
  // This is populate with UserId and the corresponding items id
  private HashMap<Integer, Double> userItems = new HashMap<Integer, Double>();
  private Integer K = 100; // for now
  
  private HashMap<Integer, TreeSet<TopKRecord>> recommendedItemBucket = new HashMap<Integer, 
      TreeSet<TopKRecord>>();
  
  private DoubleWritable similarityWritable = new DoubleWritable();
  private Text combinedKeyText = new Text();
  
  @Override
  public void reduce(Text itemIdsKey, Iterable<DoubleWritable> adjustedRatings, Context context)
          throws IOException, InterruptedException {
    
    if(itemIdsKey.toString().contains(DELIMITER)){
      // This means the key is coming from Correlation Map (itemId1, itemId2) -> value
      Double adjustedSum = 0.0;
      DoubleWritable  adjustedSumWritable = new DoubleWritable();
      
      for(DoubleWritable adjustedRating : adjustedRatings){
        // Sum the adjusted ratings
        adjustedSum += adjustedRating.get();
      }
      
      // Take the square root of the squared sum
      adjustedSumWritable.set(adjustedSum);
      // (item_ids, adjustedSumWritable) for that id
      // item_ids will again be like (item_id1, item_id2)
      //context.write(itemIds, adjustedSumWritable); 
      adjustedSumsMap.put(itemIdsKey.toString(), adjustedSumWritable.get());
     
    } else {
      // This means its the adjusted squared sums (itemId1) -> value
      
      Integer itemId = Integer.parseInt(itemIdsKey.toString());
      Double squaredAdjustedSum = 0.0;
      DoubleWritable rootSquaredAdjustedSum = new DoubleWritable();
      
      for(DoubleWritable squaredAdjustedRating : adjustedRatings){
        // Sum the adjusted ratings
        squaredAdjustedSum += squaredAdjustedRating.get();
      }
      
      // Take the square root of the squared sum
      rootSquaredAdjustedSum.set(Math.sqrt(squaredAdjustedSum));
      // (item_id, rootSquaredAsjustedSum) for that id
      rootSquaredAdjustedMap.put(itemId, rootSquaredAdjustedSum.get());
    }
  }
  

  protected void cleanup(Context context)
      throws IOException, InterruptedException {
   
    // Assuming rootSquaredAdjustedMap is populated
    // calculating similarity of a pair of items then
    for (Entry<Integer,Double> itemEntryI : rootSquaredAdjustedMap.entrySet()) {
      for (Entry<Integer,Double> itemEntryJ : rootSquaredAdjustedMap.entrySet()) {
        // for preventing duplication for the pair of keys 
        // as the relation is symmetric
        if(itemEntryI.getKey() < itemEntryJ.getKey()){
          String combinedKey = itemEntryI.getKey()+","+itemEntryJ.getKey();
          Double numerator= 
              adjustedSumsMap.get(combinedKey);
          Double denominator = rootSquaredAdjustedMap.get(itemEntryI.getKey())*
              rootSquaredAdjustedMap.get(itemEntryJ.getKey());
          Double similarity = numerator / denominator;
          combinedKeyText.set(combinedKey);
          similarityWritable.set(similarity);
          // Final output is 
          // (item1, item2) -> similarity(item1, item2)
          context.write(combinedKeyText, similarityWritable); 
          similarityMap.put(combinedKey, similarity);
          
        }
      }
    }
    calculateRecommendedItems(similarityMap);
  }
  
  private void calculateRecommendedItems(HashMap<String, Double> 
     similarityMap){
    
    // userItems can be populated and put in the Distributed Hash from the 
    // Mappers
    for(Integer itemId: userItems.keySet()){
      //look for this itemId in similarity Map
      for(String similarityPair :similarityMap.keySet()){
        if(similarityPair.startsWith(itemId+"")){
          // then get the second itemId
          String similiarItem = similarityPair.split(DELIMITER)[1];
          Double similarity = similarityMap.get(similarityPair);
          TopKRecord k = new TopKRecord(itemId, similarity);
          this.recommendedItemBucket.get(itemId).add(k);
          if(this.recommendedItemBucket.size() >= K){
            this.recommendedItemBucket.get(itemId).pollFirst();
          }
          
        }
      }
    }
  }

}
