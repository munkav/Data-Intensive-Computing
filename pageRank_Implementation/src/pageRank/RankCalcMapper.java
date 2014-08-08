package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class RankCalcMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
	{
		int pIndex = value.find("\t");
		int rIndex = value.find("\t",pIndex+1);
		String page1 = Text.decode(value.getBytes(),0,pIndex);
		String prank = Text.decode(value.getBytes(),0,rIndex+1);
		output.collect(new Text(page1), new Text("#"));
		if(rIndex == -1) return;
		String links = Text.decode(value.getBytes(), rIndex+1, value.getLength()-(rIndex+1));
        String[] allPages = links.split(",");
        int totLinks = allPages.length;
        
        for (String otherPage : allPages){
            Text pRankTotLinks = new Text(prank + totLinks);
            output.collect(new Text(otherPage), pRankTotLinks);
        }
        // Put the original links of the page for the reduce output
        output.collect(new Text(page1), new Text("|"+links));
	}
}
