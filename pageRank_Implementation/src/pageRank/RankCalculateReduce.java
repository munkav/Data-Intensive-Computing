package pageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class RankCalculateReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
private static final float beta = 0.85F;
public static final String PAGE_COUNT = "pageCount";
private int N ;

//@override
	public void configure(JobConf job)
	{
		N = Integer.parseInt( job.get(PAGE_COUNT));
	}   
	   
	public void reduce(Text page, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException 
    {	
    	boolean isExistingPage = false;
        String[] split;
        float sumOtherPageRank = 0;
        String links = "";
        String pageRank;
    	while(values.hasNext()){
    		pageRank = values.next().toString();
    		if(pageRank.equals("#")){
    			isExistingPage = true;
    			continue;
    		}
    		if(pageRank.startsWith("|")){
    			links = "\t" + pageRank.substring(1);
    			continue;
    		}
    		split = pageRank.split("\\t");
    		float pageRank1 = Float.valueOf(split[1]);
            int countOutLinks = Integer.valueOf(split[2]);
            
            sumOtherPageRank += (pageRank1/countOutLinks);
        }
        if(!isExistingPage) return;
        float newRank = beta * sumOtherPageRank + (1-beta)/N;
        out.collect(page, new Text(newRank + links));
    }
}
