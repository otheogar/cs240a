
public class TopKRecord implements Comparable<TopKRecord> {
  
  public Integer itemId;
  public Double similarityMeasure;
  
  public TopKRecord(Integer itemId, Double similarityMeasure){
    this.itemId = itemId;
    this.similarityMeasure = similarityMeasure;
  }
  
  @Override
  public boolean equals(Object obj) {
    TopKRecord other = (TopKRecord)obj;
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
    if(this.similarityMeasure == arg0.similarityMeasure){
      return this.itemId.compareTo(arg0.itemId);
    }
    return this.similarityMeasure.compareTo(arg0.similarityMeasure);
  }

}
