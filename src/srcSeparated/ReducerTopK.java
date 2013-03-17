

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.lang.StringBuffer;
import java.net.URI;
import java.lang.Math;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ReducerTopK extends Reducer<IntWritable, TopKRecord, IntWritable, Text> {
  private static Integer TOPK = 30; //find most similar 30 items
  private TreeSet<TopKRecord> recommendedItemBucket = new TreeSet<TopKRecord>();
  private Text topKList = new Text();
  
  
  private HashMap<String, Double> similarityMap = new HashMap<String, Double>();
  // This is populate with UserId and the corresponding items id
  private HashMap<Integer, ArrayList<String>> userItems = new HashMap<Integer, ArrayList<String>>();
  private HashMap<Integer, TreeSet<TopKRecord>> recommendedItemBuckets = new HashMap<Integer, 
      TreeSet<TopKRecord>>();
  private static String DELIMITER = ",";
  private static Double EPSILON = 0.000000001;
  
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
    String[] symlinks = new String[files.length];
    int i = 0;
    String symlink = "";
    if(files[0].toString().split("\\#").length >= 2){
      symlink = files[0].toString().split("\\#")[1];
    }
    
    System.out.println(symlink);
    BufferedReader in = new BufferedReader
        (new InputStreamReader(new FileInputStream(symlink)));
    String line;
    while ((line = in.readLine()) != null) {
      //System.out.println(line);
      String[] keyval = line.split("\\t");
      if (keyval.length == 2) {
        String userId = keyval[0];
        String[] entries = keyval[1].split(";"); 
        ArrayList<String> itemList = new ArrayList<String>();
        for (String e : entries) {
          String[] s = e.split(",");
          itemList.add(s[0]+","+s[1]);
        }
        userItems.put(Integer.parseInt(userId), itemList);
        //System.out.println(Integer.parseInt(userId)+" "+itemList);
        
      }
    }
  }
  
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
      similarityMap.put(itemId+","+topk.itemId.toString(), topk.similarityMeasure);
    }
    
    //topKList.set(sb.toString());
    //context.write(itemId, topKList);
    
  
  }
  
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    calculateRecommendedItems(context, similarityMap);
  }
  
  private void calculateRecommendedItems(Context context, HashMap<String, Double> 
  similarityMap) throws IOException, InterruptedException{
 
 // userItems can be populated and put in the Distributed Hash from the 
 // Mappers
 // Assumed format is key : userId value : itemId,rating 
 for(Integer userId: userItems.keySet()){
   //look for this itemId in similarity Map
   TopKRecord k = null;
   for(String similarityPair :similarityMap.keySet()){    
     Double normalizedSimilarity = 0.0, summarlizedSimilarity = 0.0;
     //Assumed format is key : userId value : itemId,rating 
     String similiarItem = similarityPair.split(DELIMITER)[1];
     for(String items: userItems.get(userId)){
       // check for neighborhood
       if(similarityMap.containsKey(items.split(DELIMITER)[0]+
           DELIMITER+similiarItem)){
         // then get the second itemId
         normalizedSimilarity += similarityMap.get(similarityPair)
             * Double.parseDouble(items.split(DELIMITER)[1]);
         summarlizedSimilarity += Math.abs(similarityMap.get(similarityPair));
        
       }
     }
   
  
    //context.write(new IntWritable(Integer.parseInt(similiarItem)),new Text(normalizedSimilarity.toString()));
    //context.write(new IntWritable(Integer.parseInt(similiarItem)),new Text(summarlizedSimilarity.toString()));
    //System.out.println("normalizedsimilarity: " + normalizedSimilarity);
    if(Math.abs(summarlizedSimilarity) < EPSILON)
      continue;
     normalizedSimilarity /= summarlizedSimilarity;
     
     //System.out.println("summarlizedsimilarity: " + summarlizedSimilarity);
     k = new TopKRecord(Integer.parseInt(similiarItem),
         normalizedSimilarity);
     if(!this.recommendedItemBuckets.containsKey(userId)){
       this.recommendedItemBuckets.put(userId, new TreeSet<TopKRecord>());  
     } 
     this.recommendedItemBuckets.get(userId).add(k);
    
    
     // Currently we are not testing whether the items we are recommending 
     // are already present in his list of elements
     if(this.recommendedItemBuckets.size() >= TOPK){
       this.recommendedItemBuckets.get(userId).pollFirst();
     }
   }
   StringBuffer sb = new StringBuffer();
   for(TopKRecord topK : this.recommendedItemBuckets.get(userId)){
     if(sb.length() != 0) {
       sb.append(";");
     }
     sb.append(topK.itemId.toString()+","+topK.similarityMeasure.toString());
     
   }
   topKList.set(sb.toString());
   IntWritable userIdWritable = new IntWritable();
   userIdWritable.set(userId);
   context.write(userIdWritable, topKList);
   
   
 }
}

}
