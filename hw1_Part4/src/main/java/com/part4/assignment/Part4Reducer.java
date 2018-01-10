package com.part4.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Part4Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int number = 0;
		for (IntWritable val : values) {
			number +=val.get();
		}
		result.set(number);
		context.write(key, result);
	}
}
