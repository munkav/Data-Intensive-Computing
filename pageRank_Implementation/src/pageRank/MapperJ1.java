package pageRank;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class MapperJ1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
{
	private final Pattern BRACKETPATTERN = Pattern.compile("\\[.+?\\]");
	private final String OPENTITLETAG = "<title>";
	private final String CLOSETITLETAG = "</title>";
	private final String OPENTEXTTAG = "<text";
	private final String CLOSETEXTAG = "</text>";
	private final String HASH = "#";
	private final String DOT = ".";
	private final String COMMA = "'";
	private final String UNDERSCORE = "_";
	private final String AND = "&";
	private final String BACKSLASH = "\\";
	private final String HYPHEN = "-";
	private final String BRACKET = "{";
	private final String COLN = ":";
	private final String SPACE = "\\s";

	
	
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
		String title = extractTitle(value); //extracting title from given key
		String text;
		Text title_text = null;
		Set<String> outLinks = new HashSet<String>(); //using set to store unique Out links for each Title

		if(title != null  && title.length() != 0)
		{
			text = extractText(value);				
			title_text = new Text(title.replaceAll(SPACE, UNDERSCORE));
			Matcher matcher = BRACKETPATTERN.matcher(text);
			
			while (matcher.find())
			{
		           String otherLinks = matcher.group();
		           otherLinks = extractwikiLink(otherLinks);
		           if(otherLinks == null || otherLinks.isEmpty())
		              continue;
		           
		           if(otherLinks.equals(title))  //Checking for self Links
		        	   continue;
		           if(!outLinks.add(otherLinks)) //Check if the link is already inserted in the output or not.
		           		continue;
		           		
		           output.collect(title_text, new Text(otherLinks));
		   }
		}
		
		 if(outLinks.size() == 0)   		//if a title has no outLinks, still it needs to be added to the result and its pageRank needs to be calculated.
			  output.collect(title_text, new Text(""));
			 
		
		
}
	
	
	
	public String extractTitle(Text value) throws CharacterCodingException
	{
		int start = value.find(OPENTITLETAG);
	    int end = value.find(CLOSETITLETAG, start);
	    start += OPENTITLETAG.length(); //add <title> length.hash
	    
	    if(start == -1 || end == -1) 
            return new String("");
	        
	    return Text.decode(value.getBytes(), start , end-start);
	    
	   
	}
	
	public String extractText(Text value) throws CharacterCodingException
	{
		int start = value.find(OPENTEXTTAG);
		start = value.find(">", start);
	    int end = value.find(CLOSETEXTAG, start);
	    start += 1; //add <title> length.
	    
	    if(start == -1 || end == -1) 
            return new String("");	        
	    return Text.decode(value.getBytes(), start, end-start);
			
	}
	
	
	public String extractwikiLink(String Link)
	
	{
		  if (Link != null && Link.isEmpty())
			  return null;
	      if(IsNotProperWikiLink(Link))
	    	  return null;
	        
	        int start = Link.startsWith("[[") ? 2 : 1;
	        
	        int end = Link.indexOf("|")  > 0 ?  Link.indexOf("|") : Link.indexOf("]");   //if Pipe is present in link end is till pipe, otherwise till ']'
	        	        
	        Link = Link.substring(start, end);
	        Link = Link.replaceAll(SPACE, UNDERSCORE);
	        Link = Link.replaceAll(COMMA, "");
	       // Link = sweetify(Link);
	        
	        return Link;
	    }
	
	  private boolean IsNotProperWikiLink(String Link) {
		
		  if (Link.isEmpty()) 
			  return true ;  
		  int start = 1;
	        if(Link.startsWith("[[")){
	            start = 2;
	        }
	      	        
	        if(( Link.startsWith(HASH, start)) || (Link.startsWith(COMMA, start))
	        ||( Link.startsWith(DOT, start)) || (Link.startsWith(AND, start))||( Link.startsWith(BACKSLASH, start))
	        || (Link.startsWith(HYPHEN, start)) ||(Link.startsWith(BRACKET, start))) 
	        	return true;
	      	        
	        if( Link.contains(COLN) || Link.contains(COMMA) || Link.contains(AND) || Link.contains(HASH)) 
	        	return true; 
	        
	        return false;
	    }
	
	
	
	
}
