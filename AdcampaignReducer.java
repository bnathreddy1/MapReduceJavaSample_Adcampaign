

import java.io.IOException; 
import java.util.StringTokenizer; 
  
/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project.
 */
 
 
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//  input key/value : IntWritable/IntWritable 
//  These class types match the output of Mapper (it is very important that Ouput datatypes  of mapper class 
//  should be same of input datatypes of Reducer class.

//And the output is IntWritable/Text

public class AdcampaignReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
{
	
	//  Key/value : IntWritable/List of IntWritables for every campaign, we are getting all actions for that 
	//  campaign as an iterable list. We are iterating through action_ids and calculating views and click 
	//  Once we are done calculating, we write out the results. This is possible because all actions for a campaign are grouped and sent to one reducer. 

	//Text k= new Text(); 
	
	public void reduce(IntWritable key, Iterable<IntWritable> results, Context context) throws IOException, InterruptedException 
   { 
		
        int campaign = key.get();
		//k = key.get();
        
        int clicks = 0;
        int views = 0;
        
        for(IntWritable i:results)
        {
        		int action = i.get();
        		if (action ==1)
        			views = views+1;
        		else if (action == 2)
        			clicks = clicks + 1;
        		
        
        }
        
        String statistics = "Total Clicks =" +clicks + "and Views =" + views;
        
        
        
        context.write(new IntWritable(campaign), new Text(statistics));
        
        
         
    }
	
}
