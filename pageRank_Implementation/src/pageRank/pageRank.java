package pageRank;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;



public class pageRank {
 
    public static void main(String[] args) throws Exception {
    	pageRank pageRanking = new pageRank();
    	 NumberFormat nf = new DecimalFormat("00");
        //In and Out dirs in HDFS
    	pageRanking.runXmlParsing("wiki", "wiki/ranking/iter00");
        pageRanking.calcTotalPages("wiki/ranking/iter00", "wiki/count");
    	
    	  int runs = 0;
          for (; runs < 1; runs++) {
              //Job 2: Calculate new rank
           pageRanking.runRankCalculation("wiki/ranking/iter"+nf.format(runs), "wiki/ranking/iter"+nf.format(runs + 1), "wiki/count/part-00000");
          }
      
         	pageRanking.runRankOrdering("wiki/ranking/iter01", "wiki/result", "wiki/count/part-00000");
    }
 
    public void runXmlParsing(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(pageRank.class);
 
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        // Mahout class to Parse XML + config
        conf.setInputFormat(XmlInputFormat.class);
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        // Our class to parse links from content.
       
        conf.setMapperClass(MapperJ1.class);
 
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        // Our class to create initial output
        conf.setReducerClass(ReducerJ1.class);
 
        JobClient.runJob(conf);
    }
    
    public void calcTotalPages(String inputPath, String outputPath) throws IOException {
    	
    		
    	   JobConf conf = new JobConf(pageRank.class);
    	   
    	  
           FileInputFormat.setInputPaths(conf, new Path(inputPath));
           // Mahout class to Parse XML + config
           conf.setInputFormat(TextInputFormat.class);
           conf.setMapOutputKeyClass(IntWritable.class);
           conf.setMapOutputValueClass(LongWritable.class);
          // conf.setInputFormat(TextInputFormat.class);        
        //   conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        //   conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
           // Our class to parse links from content.
           conf.setMapperClass(MapperJ2.class);
    
           FileOutputFormat.setOutputPath(conf, new Path(outputPath));
           conf.setOutputFormat(TextOutputFormat.class);
           conf.setOutputKeyClass(IntWritable.class);
           conf.setOutputValueClass(LongWritable.class);
           // Our class to create initial output
           conf.setReducerClass(ReducerJ2.class);
    
           JobClient.runJob(conf);
    	}
    
    
    private void runRankCalculation(String inputPath, String outputPath, String countPath) throws IOException {
    	
    	
    	JobConf conf = new JobConf(pageRank.class);
		Integer count = new Integer(readCountFromFile(countPath));  //reading Total number of links in the input from hdfs.
	
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
 
        conf.setMapperClass(RankCalcMapper.class);
        conf.setReducerClass(RankCalculateReduce.class);
        conf.set(RankCalculateReduce.PAGE_COUNT, count.toString());
 
        JobClient.runJob(conf);
    }
    
    private void runRankOrdering(String inputPath, String outputPath, String countPath) throws IOException {
        JobConf conf = new JobConf(pageRank.class);
        Integer count = new Integer(readCountFromFile(countPath));  //reading Total number of links in the input from hdfs.
        
        conf.setOutputKeyClass(FloatWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
        conf.setMapperClass(RankMapper.class);
        conf.setReducerClass(ReducerJ4.class);
        conf.set(RankMapper.PAGE_COUNT, count.toString());  //setting count value in Mapper
        JobClient.runJob(conf);
    }
    
    private int readCountFromFile(String countPath)
    {
    	
        Integer count = 0; 
        Configuration config = new Configuration();
  		Path path = new Path(countPath);
  		BufferedReader br;
  		FileSystem fs;
		try 
		{
			
			fs = path.getFileSystem(config);
			br=new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine(); 		
			if(line.length() != 0)
				count = Integer.parseInt(line.substring(line.lastIndexOf('\t')+1));
			br.close();
			fs.close();
  		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return count;
    }
}