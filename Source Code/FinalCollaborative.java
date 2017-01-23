//collaborative based recommendation
package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FinalCollaborative extends Configured implements Tool {
	
	public static final String OUTPUT = "collaboutput";
	
	public static void main(String[] args) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		int res  = ToolRunner .run( new FinalCollaborative(), args);
		  System .exit(res);
	}
	   
	public int run( String[] args) throws  Exception {
		
		String output = OUTPUT;
		//int i = 1;
		
		//job to generate the user rated movies and their rating in the format userid	movieid:rating,movieid:rating,...............
		Configuration conf1 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf1, "usermovierating");
		job1.setJarByClass(FinalCollaborative.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));//input is ratings.csv with userid,movieid,rating
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(Job1Reducer.class);
		FileOutputFormat.setOutputPath(job1, new Path(output+1));
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.waitForCompletion(true);
		
		
		//job to generate movieid:movieid pairs with their co-occurance score in the format movieid:movieid		score
		Configuration conf2 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf2, "moviepairs");
		job2.setJarByClass(FinalCollaborative.class);
		FileInputFormat.setInputPaths(job2, new Path(output+1));//input is the output of step1
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job2, new Path(output+2));
		job2.waitForCompletion(true);
		
		//job to take the first two inputs and generate output in the format movieid:movieid:userid and(countofco-occurance*userrating for that movie)
		Configuration conf3 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job3 = new Job(conf3, "combine");
		job3.setJarByClass(FinalCollaborative.class);
		MultipleInputs.addInputPath(job3, new Path(output+1), TextInputFormat.class, Job3Mapper1.class); //output from step1 format:  userid  movieid:rating, movieid:rating....
		MultipleInputs.addInputPath(job3, new Path(output+2), TextInputFormat.class, Job3Mapper2.class); //output from step2 format: movieid1:movieid2 -> count of occurrences of movie pairs 
		job3.setReducerClass(Job3Reducer.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(job3, new Path(output+3));
		job3.waitForCompletion(true);
		
		
		Configuration conf4 = new Configuration();
		Job job4 = new Job(conf4, "job4");
		job4.setJarByClass(FinalCollaborative.class);
		FileInputFormat.setInputPaths(job4, new Path(output+3));//input is output of step3
		job4.setMapperClass(Job4Mapper.class);
		job4.setMapOutputKeyClass(Comparator1.class);
		job4.setMapOutputValueClass(DoubleWritable.class);
		job4.setReducerClass(Job4Reducer.class);
		job4.setOutputKeyClass(LongWritable.class);
		job4.setOutputValueClass(Text.class);
		job4.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job4, new Path(output+4));
		job4.waitForCompletion(true);
		
		Configuration conf5 = new Configuration();
		Job job5 = new Job(conf5, "job5");
		job5.setJarByClass(FinalCollaborative.class);
		MultipleInputs.addInputPath(job5, new Path(output+1), TextInputFormat.class, Job5Mapper1.class);//input is output of step1
		MultipleInputs.addInputPath(job5, new Path(output+4), TextInputFormat.class, Job5Mapper2.class);//input is output of step4
		job5.setMapOutputKeyClass(Comparator1.class);
		job5.setMapOutputValueClass(DoubleWritable.class);
		job5.setReducerClass(Job5Reducer.class);
		job5.setOutputKeyClass(LongWritable.class);
		job5.setOutputValueClass(Text.class);
		job5.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job5, new Path(output+5));
		job5.waitForCompletion(true);
		
		
		Configuration conf6 = new Configuration();
		Job job6 = new Job(conf6, "scoresort");
		job6.setJarByClass(FinalCollaborative.class);
		FileInputFormat.setInputPaths(job6, new Path(output+5));
		job6.setMapperClass(Job6Mapper.class);
		job6.setMapOutputKeyClass(Comparator2.class);
		job6.setMapOutputValueClass(LongWritable.class);
		job6.setReducerClass(Job6Reducer.class);
		job6.setOutputKeyClass(LongWritable.class);
		job6.setOutputValueClass(Text.class);
		job6.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job6, new Path(output+6));
		return job6.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Job1Mapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");//splits userid,movieid,rating
			//contains the userid
			long finalkey = Long.parseLong(split[0]);//userid
			//movie id and ratingd seperated by :
			String tmp = split[1]+":"+split[2];//movieid:rating
			//write userid and the above string to reducer
			context.write(new LongWritable(finalkey), new Text(tmp));
		}
	}
	
	public static class Job1Reducer extends Reducer<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			String finalvalue = "";
			for (Text value : values) {
				//combine single users all movie ratings
				finalvalue = finalvalue +","+value.toString();
			}
			context.write(key, new Text(finalvalue.substring(1)));
		}
	}
	
	public static class Job2Mapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");//userid,movieid,rating
			for (int i = 1; i < split.length; i++) {
				String[] movieidrating = split[i].split(":");
				for (int j = 1; j < split.length; j++) {
					String[] movieidrating1 = split[j].split(":");
					String pairs = movieidrating[0]+":"+movieidrating1[0];
					context.write(new Text(pairs), new LongWritable(1L));//writing the movie pairs with 1, each time it occurs
				}
			}
		}
	}
	
	public static class Job2Reducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text pairs, Iterable<LongWritable> value,Context context)
				throws IOException, InterruptedException {
			Long count = 0L;
			for (LongWritable num : value) {
				count += num.get();//add the number of times the movie pairs co-occured
			}
			context.write(pairs, new LongWritable(count));//output will be movieid:movieid	co-occurance count
		}
	}
	
	public static class Job3Mapper1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]"); //splits for pattern [userid,movieid:rating,movieid:rating.....]
			for (int i = 1; i <= 100; i++) { // loop for appending all the movies to each movieid:userid pair to get the pattern (movieid)all:movieid:userid
				for (int j = 1; j < split.length; j++) {
					String[] split2 = split[j].split(":");//splits movie id and the rating
					String finalkey = i + ":" + split2[0] + ":" + split[0];//1:movieid:userid
					double result = Double.parseDouble(split2[1]);//rating
					context.write(new Text(finalkey), new DoubleWritable(result));//(movieid)1:movieid:userid		rating
				}
			}
		}
	}

	public static class Job3Mapper2 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {//1953:1031	1 input is: movieid:movieid count of occurrances
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");//movieid:movieid &&& count of co-occurances
			for (int i = 1; i <= 100; i++) {  
				String finalkey = split[0] + ":" + i;//movieid:movieid:1(all users)  similar to the output of the mapper1
				Double result = Double.parseDouble(split[1]);//count of co-occurances
				context.write(new Text(finalkey), new DoubleWritable(result));//movieid:movieid:1(userid)		count of co-occurances
			}
		}
	}

	public static class Job3Reducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> { 
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> value,Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			double result = 1;
			for (DoubleWritable single : value) {
				count++;
				result = result * single.get();
				if (count > 1) {
					context.write(key, new DoubleWritable(result));//movieid:movieid:userid		result
				}// outputs the movie pairs along with userids -> ratings*count of occurrances => score
				// so the format will be moviesid1:movieid2:userid	score
				// Step3 combines the results from user watched movies and similar movies 
			}
		}
	}
	
	public static class Job4Mapper extends
			Mapper<LongWritable, Text, Comparator1, DoubleWritable> {//movieid:movieid:userid		result
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Comparator1, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[:\t]");
			double result = Double.parseDouble(split[3]);
			context.write(new Comparator1(Long.parseLong(split[2]), Long.parseLong(split[0])) , new DoubleWritable(result)); // sends userid, movieid1 to Comparator1 to sort 
			// In the next step the output from Comparator1 which is sorted according to userid and movieid1, the score are sent to reducer
			
		}
	}
	// this reducer takes the sorted userid,movieid list   and   the scores from the mapper and sum of the scores userid:movieid pairs
	public static class Job4Reducer extends Reducer<Comparator1, DoubleWritable, LongWritable, Text> {
		@Override
		protected void reduce(Comparator1 key,Iterable<DoubleWritable> value,
				Reducer<Comparator1, DoubleWritable, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			double sum = 0;
					
			for (DoubleWritable num : value) {
				sum += num.get();
	
			}
			long finalkey = key.first;
			String result = key.second+","+sum;
			context.write(new LongWritable(finalkey), new Text(result));
			//here key is userid, value is movieid,sum of the scores userid:movieid pairs
			// output of this reducer is sorted list of userid	movieid,sum of the scores userid:movieid pairs
			// output format 1(userid)	1(movieid),12.0 (sum of the scores userid:movieid pairs)
			// Step 4 outputs the score for particular userid and movieid
		}
	}
	
	public static class Job5Mapper1 extends Mapper<LongWritable, Text, Comparator1, DoubleWritable> {
		@Override
		protected void map(LongWritable key,Text value,
				Mapper<LongWritable, Text, Comparator1, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			//value is userid	movieid:rating etc...   1	1953:4.0,2150:3.0....etc
			String[] split = value.toString().split("[\t,]"); //split -> [1,1953:4.0,2150:3.0....]
			for (int i = 1; i < split.length; i++) {	
				String[] split2 = split[i].split(":");		//split2 -> [movieid,rating]	[1953,4.0]
				double result = Double.parseDouble(split2[1]);  //result is rating value
				context.write(new Comparator1(Long.parseLong(split[0]), Long.parseLong(split2[0])), new DoubleWritable(result));
				
				//output is Comparator1(userid,movieid)
				//output of the mapper is sent to Comparator1, where it sorts according to userid. If user id is same, it will sort according to movieid
			}
		}
	}
	//input is output4.txt
	public static class Job5Mapper2 extends Mapper<LongWritable, Text, Comparator1, DoubleWritable> {
		@Override
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Comparator1, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// userid	movieid,sum of the scores userid:movieid pairs
			//value is 1(userid)	1(movieid),12.0 (sum of the scores userid:movieid pairs)
			String[] split = value.toString().split("[\t,]"); //split -> [1,1,12.0] (userid,movieid,sum of scores)
			double result = Double.parseDouble(split[2]); //result is sum of scores
			context.write(new Comparator1(Long.parseLong(split[0]), Long.parseLong(split[1])), new DoubleWritable(result)); 
			//In the next step the output from Comparator1 which is sorted according to userid and movieid, the score are sent to reducer
		}
	}
	// combines the sorted output of both mappers
	public static class Job5Reducer extends Reducer<Comparator1, DoubleWritable, LongWritable, Text> {
		@Override
		protected void reduce(Comparator1 key,Iterable<DoubleWritable> value,Context context)
				throws IOException, InterruptedException {
			Double score=0.0;
			int count=0;
			for (DoubleWritable time : value) {
				count++;
				score = time.get();
			}
			if (count == 1) {
				String result = key.second + "," + score;
				context.write(new LongWritable(key.first), new Text(result));
				// output will be in the format of userid	movieid, (sum of scores and user rated or watched movies)
				// output from step1 gives user rated movies and output from step4 gives the score for particular userid and movieid
			}
		}
	}
	
	public static class Job6Mapper extends Mapper<LongWritable, Text, Comparator2, LongWritable>{
		LongWritable result = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			// value is userid	movieid, (sum of scores and user rated or watched movies)
			//1	1,12.0
			String[] split = value.toString().split("[\t,]"); // split -> [1,1,12.0]  [userid, movieid, sum]
			result.set(Long.parseLong(split[1]));
			context.write(new Comparator2(Long.parseLong(split[0]), Double.parseDouble(split[2])), result);//userid and rating
			// Here we are bothered about the userid and Sum of scores. Based on the scores we can recommend movie ids
			// In the next step the output from Comparator1, which is sorted according to userid and (sum of scores and user rated or watched movies), the score are sent to reducer
		}
	}
	
	public static class Job6Reducer extends Reducer<Comparator2, LongWritable, LongWritable, Text>{
		LongWritable userid = new LongWritable();
		Text result = new Text();
		
		@Override
		protected void reduce(Comparator2 key, java.lang.Iterable<LongWritable> values,Context context)
				throws IOException ,InterruptedException {
			for (LongWritable movie : values) {
				userid.set(key.first);    //userid
				result.set(movie+","+key.second);  //movieid,score
				context.write(userid, result);
			}//based on the sorted userid and scores, we will get the movieids 
		};
	}
	
	public static class Comparator2 implements WritableComparable<Comparator2>{
		long first;
		double second;
		
		public Comparator2(){}
		
		public Comparator2(long first, double second){
			this.first = first;
			this.second = second;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeDouble(second);
		}

		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readDouble();
		}

		public int compareTo(Comparator2 o) {
			int comp = (int)(this.first - o.first);
			if(comp != 0){
				return comp;
			}
			return (int)(o.second - this.second);
		}
		
	}
	
	public static class Comparator1 implements WritableComparable<Comparator1> {
		long first;
		long second;

		public Comparator1() {
		}

		public Comparator1(long first, long second) {
			this.first = first;
			this.second = second;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}

		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}

		public int compareTo(Comparator1 o) {
			int comp = (int) (this.first - o.first);
			if (comp != 0) {
				return comp;
			}
			return (int) (this.second - o.second);
		}
	}
	
}