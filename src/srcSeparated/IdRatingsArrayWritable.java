package srcSeparated;
import org.apache.hadoop.io.*;

public class IdRatingsArrayWritable extends ArrayWritable{
		public IdRatingsArrayWritable() {
			super(IdAdjustedRating.class);
		}
	}
