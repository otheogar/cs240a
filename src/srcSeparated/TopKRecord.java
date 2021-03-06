import org.apache.hadoop.io.*;
import java.io.*;

public class TopKRecord implements Comparable<TopKRecord>,Writable {
  
  public Integer itemId;
  public Double similarityMeasure;
  
  //need a default constructor for Hadoop
  public TopKRecord(){
    this.itemId = -1;
    this.similarityMeasure = -1.11;
  }
  
  public TopKRecord(Integer itemId, Double similarityMeasure){
    this.itemId = itemId;
    this.similarityMeasure = similarityMeasure;
  }
  
  @Override
  public boolean equals(Object obj) {
    TopKRecord other = (TopKRecord)obj;
    //System.out.println(this.itemId+ " "+this.similarityMeasure);
    if(other.similarityMeasure == this.similarityMeasure && 
        other.itemId == this.itemId){
      return true;
    } else {
      return false;
    }
    
  }

  @Override
  public int compareTo(TopKRecord arg0) {
    // TODO Auto-generated method stub
    if(this.similarityMeasure.compareTo(arg0.similarityMeasure)==0){
      return this.itemId.compareTo(arg0.itemId);
    }
    return this.similarityMeasure.compareTo(arg0.similarityMeasure);
  }

  public void write(DataOutput out) throws IOException {
        out.writeInt(itemId);
        out.writeDouble(similarityMeasure);
    }
    
    public void readFields(DataInput in) throws IOException {
        itemId = in.readInt();
        similarityMeasure = in.readDouble();
    }

    public String toString() {
        return ""+Integer.toString(itemId) + ", " + Double.toString(similarityMeasure)+"";
    }
  
}
