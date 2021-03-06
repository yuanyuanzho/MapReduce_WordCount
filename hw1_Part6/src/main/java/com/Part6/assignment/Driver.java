package com.Part6.assignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Driver 
{
    	public static void main(String[] args) throws Exception {
    		if (args.length != 2) {
    			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
    			System.exit(-1);
    		}

    		Path inputPath = new Path(args[0]);
    		Path outputDir = new Path(args[1]);

    		// Create configuration  -- to running the job 
    		Configuration conf = new Configuration(true);

    		// Create job
    		// It allows user to configure a job, submit the job(when you submit the job, it will start running)
    		// control its execution, and query the state(actually see it on the mapper or reducer)
    		// create a new Configuration and pass it to the job  
    		Job job = Job.getInstance(conf);
    		job.setJarByClass(Part6Mapper.class);
    		

    		job.setMapperClass(Part6Mapper.class);
    		job.setReducerClass(Part6Reducer.class);
    		job.setNumReduceTasks(1);

    		// Specify key / valuex
    	
    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(IntWritable.class);
    		
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(IntWritable.class);
    		// Input
    		FileInputFormat.addInputPath(job, inputPath);
    		job.setInputFormatClass(TextInputFormat.class);

    		// Output
    		FileOutputFormat.setOutputPath(job, outputDir);

    		// Delete output if exists
    		FileSystem hdfs = FileSystem.get(conf);
    		if (hdfs.exists(outputDir))
    			hdfs.delete(outputDir, true);

    		// Execute job
    		int code = job.waitForCompletion(true) ? 0 : 1;
    		System.exit(code);

    	}
    
}
