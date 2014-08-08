package pageRank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerJ1 extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
    {
    	String rank = "1.0\t";
    	Integer count = 0;
    	boolean first = true;
        while(values.hasNext()){
            if(!first) rank += ",";
            rank += values.next().toString();
            first = false;
            count++;
        }
        	rank += count.toString();
        
        output.collect(key, new Text(rank)); 
    }
}
