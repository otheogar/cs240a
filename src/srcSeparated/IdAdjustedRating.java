package srcSeparated;
import org.apache.hadoop.io.*;

import java.io.*;

public class IdAdjustedRating implements Writable {
    public int id;
    public int adjusted_rating;

    
    public IdAdjustedRating(int id, int rating) {
        this.id = id;
        this.adjusted_rating = rating;

    }
    
    public IdAdjustedRating() {
        id = 0;
        adjusted_rating = 0;
        
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(adjusted_rating);
    }
    
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        adjusted_rating = in.readInt();
    }

    public String toString() {
        return Integer.toString(id) + ", " + Integer.toString(adjusted_rating);
    }
}
