
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//The MaxTempMapper has 4 parameters.
//The first two parameters dictate the inputs to the Mapper class. The Key and Value pair
//The third and fourth parameters tells us the output from Mapper class Key and Value 

//First parameter LongWritable = InputKey
//Second parameter Text = InputValue
//Third parameter Text = OutputKey
//Fourth parameter IntWritable = OutputValue

//First two parameters are - Input Key, Input Value. That is LongWritable and  Text

//Input Key is the line number in the log file. Since we dont care about it, we are just casting as Longwritable.
//Input value is the actual line in the log file, a java string data type, the equivalent MapReduce class is Text.

//Output Key is the IntWriteable and Output value is IntWritable. Mapper outputs are Campaign_id and  Action_Id (which is a View or Click)

//The mapper function is quite straightforward. We split the line into tokens. Extract campaign and action fields. Wrap these two as IntWritables and write them out. 
public class AdcampaignMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {	
	 
	 private long numRecords = 0; 
	 
	 @Override
     public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
		 
	 
		 String[] tokens = record.toString().split(",");
		 
		 if (tokens.length !=5)
		 {
			 System.out.println("*** invalid record  : " + record);
			 
		 }
		 
		 String actionStr = tokens[2];
		 String campaignStr = tokens[4];	 
	         
		 
		 try{
			 
			 //System.out.println("during parseint"); //used to debug 
			 System.out.println("actionStr =" + actionStr + "and campaign str = " + campaignStr);
			 
			 int actionid = Integer.parseInt(actionStr.trim());			 			 
			 int campaignid = Integer.parseInt(campaignStr.trim());
			 
		
			 //System.out.println("during intwritable"); //used to debug
			 IntWritable outputKeyFromMapper = new IntWritable(campaignid);
			 IntWritable outputValueFromMapper = new IntWritable(actionid);
			 
			 
			 context.write(outputKeyFromMapper, outputValueFromMapper);
			
		 }
		 catch(Exception e){
			 System.out.println("*** there is exception"); 
			 e.printStackTrace(); 
		 }
	
		 numRecords = numRecords+1;
		 
	 
	 }
	 
		 
	

}
