package com.part4.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Part4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
    
	//LongWritable key: is going to be input text key   value: the line we are going to process                            
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try{
            String[] fields = value.toString().split("::");
            String gender = fields[1];
            context.write(new Text(gender), one);
		}catch(ArrayIndexOutOfBoundsException ex){
			
		}

        }
}
