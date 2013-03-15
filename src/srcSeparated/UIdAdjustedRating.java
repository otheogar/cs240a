package srcSeparated;
import org.apache.hadoop.io.*;

import java.io.*;

public class UIdAdjustedRating implements Writable {
    public int uid;
    public int adjusted_rating;

    
    public UIdAdjustedRating(int uid, int rating) {
        this.uid = uid;
        this.adjusted_rating = rating;

    }
    
    public UIdAdjustedRating() {
        uid = 0;
        adjusted_rating = 0;
        
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeInt(uid);
        out.writeInt(adjusted_rating);
    }
    
    public void readFields(DataInput in) throws IOException {
        uid = in.readInt();
        adjusted_rating = in.readInt();
    }

    public String toString() {
        return Integer.toString(uid) + ", " + Integer.toString(adjusted_rating);
    }
}
