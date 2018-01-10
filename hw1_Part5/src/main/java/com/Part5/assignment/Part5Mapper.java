package com.Part5.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Part5Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("::");
		String userId =fields[0];
		Text t = new Text(userId);
		context.write(t, one);
	}
}
