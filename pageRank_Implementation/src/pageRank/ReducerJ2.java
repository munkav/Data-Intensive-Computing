package pageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerJ2  extends MapReduceBase implements Reducer<IntWritable,LongWritable,IntWritable,LongWritable>{
	
	public void reduce(IntWritable key, Iterator<LongWritable> values, OutputCollector<IntWritable, LongWritable> output, Reporter reporter) throws IOException 
    {	
		 int count = 0;
		 while(values.hasNext())
		 {   
			 values.next();
			 count++; //Reading 
	     }
		 
		 LongWritable counter = new LongWritable(count);		 
		 output.collect(key, counter); 
    }	
}
