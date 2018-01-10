package com.Part6.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Part6Reducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
		sum +=val.get();
	}
		result.set(sum);
		// context have the actual output here
		context.write(key, result);
	}
}
