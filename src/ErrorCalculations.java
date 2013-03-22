

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ErrorCalculations {
  
  public static String OUTFILE = "/home/cs240a-ucsb-33/music/prediction/predictedItems.txt";
  public static String RECORDS_DELIMETER = ";";
  public static String PER_RECORD_DELIMETER = ",";

  class PredictedRecord {
    
    Double predictedRating;
    Double actualRating;
    
    
    PredictedRecord(Double predictedRating, Double actualRating){
      this.predictedRating = predictedRating;
      this.actualRating = actualRating;
    }
    
  }
  public List<PredictedRecord> recordList= new ArrayList<PredictedRecord>() ;
  
  public static void main(String[] args){
    BufferedReader in = null;
    ErrorCalculations eC = new ErrorCalculations();
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(OUTFILE)));
    } catch (FileNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    String input = "";
    try {
      while((input=in.readLine())!=null){
        String[] entries = input.split("\\t");
        String [] records = entries[1].split(RECORDS_DELIMETER);
        for(String predictedItem : records){
          String[] recordEntry = predictedItem.split(PER_RECORD_DELIMETER);
          eC.addToPredictedList(Double.parseDouble(recordEntry[1]), 
              Double.parseDouble(recordEntry[2]));
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    System.out.println("RMSE is "+eC.calculateRMSE());
    System.out.println("MAE is "+eC.calculateMAE());
  }
  
  public void addToPredictedList(Double prediction, Double actualRating){
    PredictedRecord predRecord = new PredictedRecord(prediction, actualRating);
    this.recordList.add(predRecord);
  }
  
  public Double calculateRMSE(){
    
    Double error = 0.0;
    int numRecords = recordList.size();
    for(PredictedRecord predictedRecord : recordList){
	
       Double temp = 0.0;
       if(predictedRecord.predictedRating!=.01)
       		temp = Math.abs(predictedRecord.predictedRating - predictedRecord.actualRating);
       else 
	       	numRecords --;
       	error += temp*temp;
    }
    
    // Calculate the root of the squared root
    error = Math.sqrt((error / numRecords));
    
    return error;
  }
  
  public Double calculateMAE(){
    
    Double error = 0.0;
    int numRecords = recordList.size();
    for(PredictedRecord predictedRecord : recordList){
	if(predictedRecord.predictedRating!=.01)
     		 error += Math.abs(predictedRecord.predictedRating - predictedRecord.actualRating);
	else
		 numRecords --;
    }
    
    // Calculate the root of the squared root
    error = error / numRecords;
    
    return error;  
  }

}
