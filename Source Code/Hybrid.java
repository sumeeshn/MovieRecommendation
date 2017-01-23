//hybrid method of recommendation
package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Hybrid {

	public static void main(String[] args) throws Exception{
		//job to combine content and collaborative
		Configuration conf = new Configuration();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "hybrid");
		job.setJarByClass(Hybrid.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, HybridMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, HybridMapper2.class);
		job.setReducerClass(HybridReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}

	public static class HybridMapper1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");
			String userid = split[0];
			String movieid = split[1];
			String result = userid+":"+movieid;
			double score = Double.parseDouble(split[2]);
				
			context.write(new Text(result), new DoubleWritable(score));
			
		}
	}

	public static class HybridMapper2 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");
			String userid = split[0];
			String movieid = split[1];
			String result = userid+":"+movieid;
			double score = Double.parseDouble(split[2]);
				
			context.write(new Text(result), new DoubleWritable(score));
			
		}
	}

	
	//reducer to combine both the scores
	public static class HybridReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		protected void reduce(Text k2, Iterable<DoubleWritable> v2s,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double finalresult = 1.0;
			for (DoubleWritable single : v2s) {
				finalresult = finalresult*single.get();
			}
			context.write(k2, new DoubleWritable(finalresult));
		}
	}
}
