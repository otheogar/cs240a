

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
import org.apache.hadoop.mapreduce.*;




public class MapperPredict extends Mapper<LongWritable, Text, IntWritable, Text > {
		
  private IntWritable uid = new IntWritable();
  private Text predictionList = new Text();

  // This holds the neighboring items for each item
  private HashMap<Integer, HashMap<Integer,Double>> neighborhoods = new HashMap<Integer, HashMap<Integer,Double>>(); 
  private HashMap<Integer, HashMap<Integer,Double>> predictedItems = new HashMap<Integer, HashMap<Integer,Double>>();


   @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
	
	//get cache file containing uid: item to predict,actual rating,date,time 
    URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
    String symlink="";
    for(URI f: files) {
      if(f.toString().split("\\#").length >= 2){
	symlink = f.toString().split("\\#")[1];
	System.out.println(symlink);
	BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(symlink)));
	String line;
	while ((line = in.readLine()) != null) {
	  //System.out.println(line);
	  String[] keyval = line.split("\\t");
	  if (keyval.length == 2) {
	    if(symlink.contains("Ratings")) {
	      Integer userId = Integer.parseInt(keyval[0]);
	      String[] entries = keyval[1].split(";"); 
	      HashMap<Integer,Double> items = new HashMap<Integer,Double>();
	      for (String e : entries) {
		String[] s = e.split(",");
		Integer itId = Integer.parseInt(s[0]);
		Double rating = Double.parseDouble(s[1]);
		items.put(itId,rating);
		
	      }
	      predictedItems.put(userId,items);
	    }
	    else if(symlink.contains("Items")) {
	      Integer itemId = Integer.parseInt(keyval[0]);
	      System.out.println("itemId: " + itemId);
	      String[] entries = keyval[1].split(";");
	      //create a hashmap to hold similar item : similarity measure
	      HashMap<Integer,Double> neighbors = new HashMap<Integer,Double>();
	      for (String e : entries) {
		String[] s = e.split(",");
		Integer itId = Integer.parseInt(s[0]);
		Double symilarity = Double.parseDouble(s[1]);
		neighbors.put(itId,symilarity);
	      }
	      neighborhoods.put(itemId,neighbors);
	    }   
	  }
	}
      }  
    }
    
    
    
  }
		
  @Override
  public void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException {
                                
      String line = value.toString();
      String[] keyval = line.split("\\t");
      
      Integer activeUser = Integer.parseInt(keyval[0]);
      uid.set(activeUser);
      if(keyval.length != 2){
                      return;
      }
      String[] entries = keyval[1].split(";"); //items
      //create new hash to hold this user's ratings
      //itemId : rating
      HashMap<Integer,Double> ratedItems = new HashMap<Integer,Double>();
      for (String e : entries) {
                      String[] s = e.split(",");
                      Integer item_id = Integer.parseInt(s[0]);
                      Double rating = Double.parseDouble(s[1]);
                      ratedItems.put(item_id,rating);
      }
  
      //get hashmap of items to be predicted
      HashMap<Integer,Double> h = predictedItems.get(activeUser);
      StringBuffer sb = new StringBuffer();
      for(Integer itemId : h.keySet()) {
	if(sb.length() !=0){
	  sb.append(";");
	}
	//try to predict the rating for this item
	Double prediction = -1.111; //-1 indicates we couldn't find similar items rated by this user so cannot make a prediction
	Double numerator = 0.0;
	Double denominator = 0.0;
	//get the items similar to this item
	System.out.println("user " + activeUser + " item to predict: " + itemId);
	HashMap<Integer,Double> similarItems = neighborhoods.get(itemId);
	//see if it's not null
	if(similarItems != null) {
	  System.out.println("got similarity map");
	  for (Entry<Integer,Double> itemSimilarity : similarItems.entrySet()) {
	  //for each neighbor check to see if this user has rated it
	  Integer item = itemSimilarity.getKey();
	  System.out.println("similar item: " + item);
	  if(ratedItems.containsKey(item)) {
	    Double rating = ratedItems.get(item);
	    System.out.println("rating : " + rating);
	    Double similarity = itemSimilarity.getValue();
	    System.out.println("similarity: " + similarity);
	    numerator += similarity * rating;
	    System.out.println("numerator: " + numerator);
	    denominator += Math.abs(similarity);
	    System.out.println("denominator " + denominator);
	  }
	  }
	  if(denominator != 0.0) {
	   prediction = numerator/denominator;
	  }
	}
	
	System.out.println("prediction: " + prediction);
	//append to the string for output: itemPredicted, prediction, real rating (form test file)
	sb.append(itemId.toString() + "," + prediction.toString() + "," + h.get(itemId));
      }
      
      predictionList.set(sb.toString());
      context.write(uid, predictionList);
      
  
  }

}
