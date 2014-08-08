package pageRank;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;




public class ReducerJ4 extends MapReduceBase implements  Reducer<FloatWritable, Text, Text, FloatWritable>{

	
	
	public void reduce(FloatWritable key, Iterator<Text> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException 
    {		 
		 output.collect(values.next(), new FloatWritable(key.get()*-1)); 
    }

	
}
