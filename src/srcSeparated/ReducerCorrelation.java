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

public class ReducerCorrelation extends Reducer<Text, DoubleWritable, IntWritable, TopKRecord> {
  
  private HashMap<Integer, Double> squaredAdjustedMap = new HashMap<Integer, Double>();
  
  private IntWritable idiWritable = new IntWritable(0);
  private IntWritable idjWritable = new IntWritable(0);
  
  
  @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {

        URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
        String[] symlinks = new String[files.length];
        int i = 0;
        for(URI f: files) {
          System.out.println(f.toString());
          String[] s = f.toString().split("\\#");
          if(s.length != 2)
            throw new FileNotFoundException("no symlink found!!");
          System.out.println(s[1]);
          symlinks[i++] = s[1];
        }
        BufferedReader in = null;
        for(String symlink : symlinks)
          try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(symlink)));
            String line;
            String[] entries;
            while ((line = in.readLine()) != null) {
              entries = line.split("\\t");
              if (entries.length == 2) {
                squaredAdjustedMap.put(Integer.parseInt(entries[0]),Double.parseDouble(entries[1]));
              }
            }
          } finally {
              IOUtils.closeStream(in);
            }
      
    }
  
  
  @Override
  public void reduce(Text itemIds, Iterable<DoubleWritable> adjustedRatings, Context context)
          throws IOException, InterruptedException {
    
    Double adjustedSum = 0.0;
    
    //get the ids as string then parse as integers and get the norms for each id from the map 
    String[] items = itemIds.toString().split(",");
    Integer id_i = Integer.parseInt(items[0]);
    Integer id_j = Integer.parseInt(items[1]);
    Double norm_i =  Math.sqrt(squaredAdjustedMap.get(id_i));
    Double norm_j =  Math.sqrt(squaredAdjustedMap.get(id_j));
    
    for(DoubleWritable adjustedRating : adjustedRatings){
      // Sum the correlations
      adjustedSum += adjustedRating.get();
    }
    //printout for debugging
    /*
    System.out.println("adjustedSum: "+ adjustedSum);
    System.out.println("norm_i: " + norm_i);
    System.out.println("norm_j: " + norm_j);*/
    Double similarity = 0.0;
    if(norm_i > 0.0 && norm_j > 0.0)
      similarity = adjustedSum /(norm_i * norm_j);
    //System.out.println("symilarity: " + similarity);
    
    
    idiWritable.set(id_i);
    idjWritable.set(id_j);
    
    //output (id_i,(id_j,similarity(i,j)))
    TopKRecord similarityRecord = new TopKRecord(id_j,similarity);
    context.write(idiWritable, similarityRecord);
    
    //output (id_j,(id_i,similarity(i,j)))
    similarityRecord = new TopKRecord(id_i,similarity);
    context.write(idjWritable, similarityRecord); 
  }
  

}
