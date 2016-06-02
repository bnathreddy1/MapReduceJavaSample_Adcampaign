
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

//This driver program will bring all the information needed to submit this Map reduce job.

public class Adcampaign {
	
	 public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: MaxClosePrice <input path> <output path>");
	            System.exit(-1);
	        }
	 
	 
	        //To sumbit a mapreduce job we need the following information.
	        	//a. Input location where the input dataset is 
	        	//b. Output location where the mapreduce job should output the results to 
	        	//c. Name of the Mapper class that should be executed
	        	//d. Name of the reducer class that should be executed (if reducer is needed)
	        
	        //reads the default configuration of cluster from the configuration xml files
	        // https://www.quora.com/What-is-the-use-of-a-configuration-class-and-object-in-Hadoop-MapReduce-code
	         
	        Configuration conf = new Configuration();
	         
	
	        //Initializing the job with the default configuration of the cluster
	        //When we submit a mapreduce job, it will be distributed across all the nodes in the cluster. So we need give the job a name
	         
	        Job job = new Job(conf, "Adcampaign");
	        
	        //Assigning the driver class name 	        
	        job.setJarByClass(Adcampaign.class);
	        
	        
	        //first argument is job itself
	        //second argument is location of the input dataset
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        
	        //first argument is the job itself
	        //second argument is the location of the output path	    
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
    
	        
	        //Defining input Format class which is responsible to parse the dataset into a key value pair 	
	        //Configuring the input/output path from the filesystem into the job
	        // InputFormat is responsible for 3 main tasks.
	        // 		a. Validate inputs - meaning the dataset exists in the location specified.
	        // 		b. Split up the input files into logical input splits. Each input split will be assigned a mapper.
	        //		c. Recordreader implementation to extract logical records
	        
	        job.setInputFormatClass(TextInputFormat.class);
	         
	        //Defining output Format class which is responsible to parse the final key-value output from MR framework to a text file into the hard disk	   
	        //OutputFomat does 2 mains things
	        //	a. Validate output specifications. Like if the output directory already exists? If the directory exist, it will throw an error.
	        //  b. Recordwriter implementation to write output files of the job
	        //Hadoop comes with several output format implemenations.
	        
	        job.setOutputFormatClass(TextOutputFormat.class);
	        
	      
	        //Defining the mapper class name	        
	        job.setMapperClass(AdcampaignMapper.class);
	        
	        //Defining the Reducer class name
	        job.setReducerClass(AdcampaignReducer.class);
	        
	        //setting the second argument as a path in a path variable	         
	        Path outputPath = new Path(args[1]);
	        
	        //deleting the output path automatically from hdfs so that we don't have delete it explicitly	         
	        outputPath.getFileSystem(conf).delete(outputPath);
	        
	        //Output types	
	        //Ouput key from the mapper class
	        job.setMapOutputKeyClass(IntWritable.class);
	        
	        //Output key from the reducer class
	        job.setMapOutputValueClass(IntWritable.class);
	        
	        ///exiting the job only if the flag value becomes false
	         
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	        
	        
	 }
	 
}
