package com.Part6.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Part6Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
                                
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
               
            String[] fields = value.toString().split("- -");
            String ip = fields[0];
            context.write(new Text(ip), one);
 
	
        }
}
