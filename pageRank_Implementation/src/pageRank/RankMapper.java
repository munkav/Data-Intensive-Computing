package pageRank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankMapper extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> {
    
    public static final String PAGE_COUNT = "pageCount";
    private int N = 0;
    private float SELECTFACTOR = 5;
    
     
	public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter arg3) throws IOException {
    	String[] pAndR = getPageAndRank(key, value);
        float pFloat = Float.parseFloat(pAndR[1]);     
        Text page = new Text(pAndR[0]);
        if(pFloat > (SELECTFACTOR/N))
        {
        	FloatWritable rank = new FloatWritable(-pFloat);
        	output.collect(rank, page);
        }
        
}
	
    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
    	String []pAndR = new String[2];
    	int pIndex = value.find("\t");
    	int prIndex = value.find("\t",pIndex+1);
    	int end;
    	if(prIndex == -1)
    		end = value.getLength() - (pIndex + 1);
    	else
    		end = prIndex - (pIndex + 1);
    	pAndR[0] = Text.decode(value.getBytes(),0,pIndex);
    	pAndR[1] = Text.decode(value.getBytes(),pIndex+1,end);
    	return pAndR;
    }
    
    
    public void configure(JobConf job)
	{
		N = Integer.parseInt( job.get(PAGE_COUNT));
	} 
   /* public static class DecreasingComparator extends WritableComparator {
        protected DecreasingComparator() {
            super(Text.class, true);
        }
        public int compare(WritableComparable w1, WritableComparable w2) {
            LongWritable key1 = (LongWritable) w1;
            LongWritable key2 = (LongWritable) w2;          
            return -1 * key1.compareTo(key2);
        }
    }*/
    
    
    
    
}