package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperJ2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, LongWritable > {

	
	
	public void map(LongWritable key, Text value, OutputCollector< IntWritable, LongWritable> output, Reporter reporter) throws IOException {	
		
		
		IntWritable count = new IntWritable(1);
		output.collect(count, key);	  //writing count = 1 as key in the Mapper Phase, This will be used in Reducer phase to count number of pages 
	
	}
	
	
}
