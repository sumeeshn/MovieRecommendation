package org.myorg;

//Content based Movie Recommendation system

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.WritableComparator;


public class FinalContent extends Configured implements Tool {
	
	public static final String OUTPUT = "contentoutput";
	
	public static void main(String[] args) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		int res  = ToolRunner .run( new FinalContent(), args);
		  System .exit(res);
	}
	   
	public int run( String[] args) throws  Exception {
		String output = OUTPUT;
		
		//job to generate normalized genre scores for the movies(each genre is divided by 1/sqrt(total genres))
		Configuration conf1 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf1, "step1");
		job1.setJarByClass(FinalContent.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));//input is movies.csv
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(Job1Reducer.class);
		FileOutputFormat.setOutputPath(job1, new Path(output+1));//output is movie genres normalized(movieid genre:scores)
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);
		
		//job for generating (1 or -1 ) based on user rating (>=2.5 means +1 and <2.5 means -1)
		Configuration conf2 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf2, "step2");
		job2.setJarByClass(FinalContent.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]));//input is ratings.csv
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job2, new Path(output+2));//output is user movie 1 or -1
		job2.waitForCompletion(true);
		
		//job for attaching the (+1 or -1) to the step1 
		Configuration conf3 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job3 = new Job(conf3, "step3");
		job3.setJarByClass(FinalContent.class);
		MultipleInputs.addInputPath(job3, new Path(output+2), TextInputFormat.class, Job3Mapper1.class);
		MultipleInputs.addInputPath(job3, new Path(output+1), TextInputFormat.class, Job3Mapper2.class);
		job3.setReducerClass(Job3Reducer.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job3, new Path(output+3));
		job3.waitForCompletion(true);
		
		//job for multiplying (+1 or -1 ) with the movie genre scores for an user
		Configuration conf4 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job4 = new Job(conf4, "step4");
		job4.setJarByClass(FinalContent.class);
		FileInputFormat.setInputPaths(job4, new Path(output+3));
		job4.setMapperClass(Job4Mapper.class);
		job4.setReducerClass(Job4Reducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(job4, new Path(output+4));
		job4.waitForCompletion(true);
		
		//job for generating the idf scores using movies.csv file
		Configuration conf5 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job5 = new Job(conf5, "step5");
		job5.setJarByClass(FinalContent.class);
		FileInputFormat.setInputPaths(job5, new Path(args[0]));//input is movies.csv to calculate the idf scores
		job5.setInputFormatClass(TextInputFormat.class);
		job5.setMapperClass(Job5Mapper.class);
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(LongWritable.class);
		job5.setReducerClass(Job5Reducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(job5, new Path(output+5));//contains the idf scores for the genres
		job5.setOutputFormatClass(TextOutputFormat.class);
		job5.waitForCompletion(true);
		
		//for getting user genre scores in a line(input is output of step4)
		Configuration conf6 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job6 = new Job(conf6, "step6");
		job6.setJarByClass(FinalContent.class);
		FileInputFormat.setInputPaths(job6, new Path(output+4));
		job6.setInputFormatClass(TextInputFormat.class);
		job6.setMapperClass(Job6Mapper.class);
		job6.setMapOutputKeyClass(LongWritable.class);
		job6.setMapOutputValueClass(Text.class);
		job6.setReducerClass(Job6Reducer.class);
		FileOutputFormat.setOutputPath(job6, new Path(output+6));//user genre scores in a line
		job6.setOutputFormatClass(TextOutputFormat.class);
		job6.waitForCompletion(true);
		
		//for multiplying genre idf scores with user genre scores and attaching with movie genre scores
		Configuration conf7 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job7 = new Job(conf7, "step7");
		job7.setJarByClass(FinalContent.class);
		MultipleInputs.addInputPath(job7, new Path(output+6), TextInputFormat.class, Job7Mapper1.class);
		MultipleInputs.addInputPath(job7, new Path(output+1), TextInputFormat.class, Job7Mapper2.class);
		job7.setReducerClass(Job7Reducer.class);
		job7.setOutputFormatClass(TextOutputFormat.class);
		job7.setOutputKeyClass(Text.class);
		job7.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job7, new Path(output+7));
		job7.waitForCompletion(true);
		
		
		//job for eliminating already seen movies by the user from the final recommendation
		Configuration conf9 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job9 = new Job(conf9,"step9");
		job9.setJarByClass(FinalContent.class);
		MultipleInputs.addInputPath(job9, new Path(args[1]), TextInputFormat.class, Job9Mapper1.class);
		MultipleInputs.addInputPath(job9, new Path(output+7), TextInputFormat.class, Job9Mapper2.class);
		job9.setMapOutputKeyClass(Comparator1.class);
		job9.setMapOutputValueClass(Text.class);
		job9.setReducerClass(Job9Reducer.class);
		job9.setOutputKeyClass(Text.class);
		job9.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job9, new Path(output+8));
		job9.waitForCompletion(true);
		
		//for multiplying both the user and movie based genre scores and sorting in descending order according to the score
		Configuration conf8 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job8 = new Job(conf8, "step8");
		job8.setJarByClass(FinalContent.class);
		FileInputFormat.setInputPaths(job8, new Path(output+8));
		job8.setMapperClass(Job8Mapper.class);
		job8.setReducerClass(Job8Reducer.class);
		job8.setOutputFormatClass(TextOutputFormat.class);
		job8.setOutputKeyClass(Text.class);
		job8.setOutputValueClass(DoubleWritable.class);
		//job8.setSortComparatorClass(IntComparator.class);
		job8.setMapOutputKeyClass(Comparator2.class);
		//job8.setMapOutputKeyClass(DoubleWritable.class);
		job8.setMapOutputValueClass(Text.class);
		job8.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job8, new Path(output+9));
		return job8.waitForCompletion(true) ? 0 : 1;
	}
	
	//input to the mapper is movies.txt
	public static class Job1Mapper extends Mapper<LongWritable, Text, LongWritable,Text>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text,LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String[] genres = {"Action","Adventure","Animation","Children","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western","(no genres listed)"};
			//k2 contains the movieid
			long k2 = Long.parseLong(split[0]);
			
			int length = split.length -1;
			String[] genresplit = split[length].split("\\|");
			String append="";
			
			for(int i=0; i<genres.length; i++){
				double score = 0.0;
				for (int j = 0; j < genresplit.length; j++) {
					if(genres[i].equals(genresplit[j]))
					{
						score=(double)1/Math.sqrt(genresplit.length);//to get the normalized score for each genre
					}
				}
				append = genres[i]+":"+score+","+append;;
			}
			context.write(new LongWritable(k2), new Text(append));
		}
	}
	
	public static class Job1Reducer extends Reducer<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,
				Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String result = "";
			for (Text genre : v2s) {
				result=result+genre;
			}
			context.write(k2, new Text(result));
		}
	}
	
	//input is ratings.txt
	public static class Job2Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//System.out.println("reached mapper");
			String[] linesSplit = value.toString().split(",");
		    String userId = linesSplit[0];
		    int userRating = -1;
		    if(Double.parseDouble(linesSplit[2])>=2.5){//a limit for negative and positive rating
		    	userRating=1;
		    }
		    String userMovie= userId+":"+linesSplit[1];
		    System.out.println(userMovie+" and "+userRating);
		    context.write(new Text(userMovie),new IntWritable(userRating) );
			}
	}
	
	public static class Job2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text k2, Iterable<IntWritable> v2s,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("reached reducer");
			int count=0;
			for (IntWritable num : v2s) {
				count += num.get();
			}
			context.write(k2, new IntWritable(count));
		}
	}
	
	//input is the output of above step
	public static class Job3Mapper1 extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new Text(split[0]), new Text(split[1]));			
		}
	}

	//input is output of job1
	public static class Job3Mapper2 extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			for (int i = 1; i <= 50; i++) {
				String k2 = i + ":" + split[0];
				context.write(new Text(k2), new Text(split[1]));
			}
		}
	}

	//reducer to append +1 or -1 to the genres scores
	public static class Job3Reducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			String result = "";
			String rating = "";
			String genre = "";
			for (Text single : v2s) {
				count++;
				if(single.toString().equals("1")||single.toString().equals("-1"))
					rating = "###" + single;
				else 
					{
						genre = single.toString();
					}
				result = genre + rating;
						
				}
				if (count > 1) {
					context.write(k2, new Text(result));
				}
			}	
	}
	
	
	//mapper for multiplying (+1 or -1 ) with the movie genre scores for an user
	public static class Job4Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			String[] usermoviesplit = split[0].split(":");
			String user=usermoviesplit[0];
			String movie=usermoviesplit[1];
			String[] genresrating = split[1].split("###");
			String genres = genresrating[0];
			int rating = Integer.parseInt(genresrating[1]);
			String[] genresplit=genres.trim().split(",");
			
			for (int j = 0; j < genresplit.length; j++) {
				String[] genrescore = genresplit[j].split(":");
				String genre = genrescore[0];
				double score = Double.parseDouble(genrescore[1]);
				score = score*rating;
				String k = user+":"+genre;
				
				context.write(new Text(k), new DoubleWritable(score));
			}
		}
	}
	
	public static class Job4Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		protected void reduce(Text k2, Iterable<DoubleWritable> v2s,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			Double count = 0.0;
			for (DoubleWritable num : v2s) {
				count += num.get();
			}
			context.write(k2, new DoubleWritable(count));
		}
	}

	
	//mapper for generating the idf scores using movies.csv file
	public static class Job5Mapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text,Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			int length = split.length -1;
			String[] genresplit = split[length].split("\\|");
			
			for (int j = 0; j < genresplit.length; j++) {
				context.write(new Text(genresplit[j]), new LongWritable(1L));
			}
		}
	}
	
	public static class Job5Reducer extends Reducer<Text, LongWritable, Text, DoubleWritable>{
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			Long count = 0L;
			for (LongWritable num : v2s) {
				count += num.get();
			}
			double finalcount = Math.log(40000/count);//total number of movies in movies.csv
			context.write(k2, new DoubleWritable(finalcount));
		}
	}
	
	//mapper for getting user genre scores in a line(input is output of step4)
	public static class Job6Mapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			//System.out.println(split[0]+split[1]);
			String[] usergenre = split[0].split(":");
			double score = Double.parseDouble(split[1]);
			long k2 = Long.parseLong(usergenre[0]);
			String tmp = usergenre[1]+":"+score;
			context.write(new LongWritable(k2), new Text(tmp));
		}
	}
	
	public static class Job6Reducer extends Reducer<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,
				Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String v3 = "";
			for (Text v2 : v2s) {
				//combine single users all movie ratings
				v3 = v3+","+v2.toString();
				//System.out.println(v2.toString());
			}						
			context.write(k2, new Text(v3.substring(1)));
		}
	}
	
	//mapper for multiplying genre idf scores with user genre scores and attaching with movie genre scores
	public static class Job7Mapper1 extends
			Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			HashMap<String,Double> hm = new HashMap<String,Double>();
			hm.put("(no genres listed)", new Double(2.9444389791664403));
			hm.put("Action",new Double(1.9459101490553132));
			hm.put("Adventure",new Double(2.5649493574615367));
			hm.put("Animation",new Double(3.1780538303479458));
			hm.put("Children",new Double(3.044522437723423));
			hm.put("Comedy",new Double(1.0986122886681098));
			hm.put("Crime",new Double(2.302585092994046));
			hm.put("Documentary",new Double(2.3978952727983707));
			hm.put("Drama",new Double(0.6931471805599453));
			hm.put("Fantasy",new Double(2.995732273553991));
			hm.put("Film-Noir",new Double(4.736198448394496));
			hm.put("Horror",new Double(2.302585092994046));
			hm.put("IMAX",new Double(5.313205979041787));
			hm.put("Musical",new Double(3.6109179126442243));
			hm.put("Mystery",new Double(2.9444389791664403));
			hm.put("Romance",new Double(1.9459101490553132));
			hm.put("Sci-Fi",new Double(2.772588722239781));
			hm.put("Thriller",new Double(1.791759469228055));
			hm.put("War",new Double(3.295836866004329));
			hm.put("Western",new Double(3.713572066704308));
			
			String[] split = value.toString().split("[\t,]");//movie and genre:score
			for (int i = 1; i <= 50; i++) {//total movies
				String v3 = "";
				String k2="";
				String v2 = "";
				for (int j = 1; j < split.length; j++) {
					String[] genrescore = split[j].split(":");//splits genre and score
					double score = Double.parseDouble(genrescore[1]);
					double hashscore = hm.get(genrescore[0]);
					score = score*hashscore;
					k2 =  split[0]+":"+i;//userid:movieid
					v2 = genrescore[0]+":"+score;
					v3 = v3+","+v2.toString();					
				}
				context.write(new Text(k2), new Text(v3.substring(1)));//userid:movieid	genre:rating
			}
		}
	}

	public static class Job7Mapper2 extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");//user and genre:score
			for (int i = 1; i <= 50; i++) {//total user
				String k2="";
				String v3="";
				for (int j = 1; j < split.length; j++) {
					
					k2 = i+":"+split[0] ;//userid:movieid
					String v2 = split[j];
					v3=v3+","+v2;					
				}
				context.write(new Text(k2), new Text(v3.substring(1)));//userid:movieid	genre:rating
			}
		}
	}

	public static class Job7Reducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			double result = 1;
			String s = "";
			for (Text single : v2s) {
				count++;
				s = s+","+single;
				if (count > 1) {
					context.write(k2, new Text(s.substring(1)));//movieid:movieid:userid		result
				}
			}
		}
	}
	
	//for multiplying both the user and movie based genre scores and sorting in descending order according to the score
	public static class Job8Mapper extends
			Mapper<LongWritable, Text, Comparator2,Text> {
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Comparator2,Text>.Context context)
				throws IOException, InterruptedException {
			
			HashMap<String,Double> hm = new HashMap<String,Double>();
			hm.put("(no genres listed)", new Double(1.0));
			hm.put("Action",new Double(1.0));
			hm.put("Adventure",new Double(1.0));
			hm.put("Animation",new Double(1.0));
			hm.put("Children",new Double(1.0));
			hm.put("Comedy",new Double(1.0));
			hm.put("Crime",new Double(1.0));
			hm.put("Documentary",new Double(1.0));
			hm.put("Drama",new Double(1.0));
			hm.put("Fantasy",new Double(1.0));
			hm.put("Film-Noir",new Double(1.0));
			hm.put("Horror",new Double(1.0));
			hm.put("IMAX",new Double(1.0));
			hm.put("Musical",new Double(1.0));
			hm.put("Mystery",new Double(1.0));
			hm.put("Romance",new Double(1.0));
			hm.put("Sci-Fi",new Double(1.0));
			hm.put("Thriller",new Double(1.0));
			hm.put("War",new Double(1.0));
			hm.put("Western",new Double(1.0));
			
			String[] split = value.toString().split("[\t,]");//user:movie and genre:score
			String[] usermoviesplit = split[0].split(":");
			String userid = usermoviesplit[0];
			String movieid = usermoviesplit[1];
			for (int j = 1; j < split.length; j++) {
				String[] genrescore = split[j].split(":");//splits genre and score
				double score = Double.parseDouble(genrescore[1]);
				double hashscore = hm.get(genrescore[0]);
				score = score*hashscore;
				hm.put(genrescore[0],new Double(score));
			}
			double finalvalue = 100.0;
			for (double d : hm.values()){
				finalvalue = finalvalue + d;
			}
			context.write(new Comparator2(Long.parseLong(userid), finalvalue), new Text(movieid));			
		}
	}


	public static class Job8Reducer extends
			Reducer< Comparator2,Text, LongWritable, Text> {
		
		@Override
		protected void reduce(Comparator2 k2, Iterable<Text> v2s,
				Reducer<Comparator2, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {			
			for (Text single : v2s) {
				String s = single.toString();
				String moviescore = s+","+k2.second;
					context.write(new LongWritable(k2.first), new Text(moviescore));
			}
		}
	}
	
	//input is ratings.csv
	//job for eliminating already seen movies by the user from the final recommendation
	public static class Job9Mapper1 extends Mapper<LongWritable, Text, Comparator1, Text> {
		@Override
		protected void map(LongWritable key,Text value,
				Mapper<LongWritable, Text, Comparator1, Text>.Context context)
				throws IOException, InterruptedException {
			//value is userid	movieid:rating etc...   1	1953:4.0,2150:3.0....etc
			String[] split = value.toString().split("[,]"); //split -> [1,1953:4.0,2150:3.0....]
			context.write(new Comparator1(Long.parseLong(split[0]), Long.parseLong(split[1])), new Text(split[2]));
			
		}
	}
	
	//input is the output from previous step
	public static class Job9Mapper2 extends Mapper<LongWritable, Text, Comparator1, Text> {
		@Override
		protected void map(LongWritable key,Text value,
				Mapper<LongWritable, Text, Comparator1, Text>.Context context)
				throws IOException, InterruptedException {
			// userid	movieid,sum of the scores userid:movieid pairs
			//value is 1(userid)	1(movieid),12.0 (sum of the scores userid:movieid pairs)
			String[] split = value.toString().split("[\t]"); //split -> [1,1,12.0] (userid,movieid,sum of scores)
			String[] split2 = split[0].split(":");
			context.write(new Comparator1(Long.parseLong(split2[0]), Long.parseLong(split2[1])), new Text(split[1])); 
			//In the next step the output from Comparator1 which is sorted according to userid and movieid, the score are sent to reducer
		}
	}
	// combines the sorted output of both mappers
	public static class Job9Reducer extends Reducer<Comparator1, Text, Text, Text> {
		@Override
		protected void reduce(Comparator1 k2,Iterable<Text> v2s,
				Reducer<Comparator1, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> list = new ArrayList<String>();
			for (Text time : v2s) {
				list.add(time.toString());
			}
			if ((list.size() == 1)&&list.get(0).length()>20)  {
				String finalkey=k2.first+":"+k2.second;
				String v3 = list.get(0);
				context.write(new Text(finalkey), new Text(v3));//
				// output will be in the format of userid	movieid, (sum of scores and user rated or watched movies)
				// output from step1 gives user rated movies and output from step4 gives the score for particular userid and movieid
			}
		}
	}
	
	public static class IntComparator extends WritableComparator {

		public IntComparator() {
			super(IntWritable.class);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			Integer value1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer value2 = ByteBuffer.wrap(b2, s2, l2).getInt();
			return value1.compareTo(value2) * (-1);
		}
	}
	
	//comparator to compare and sort the values
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
			int minus = (int)(this.first - o.first);
			System.out.println("minus is "+minus);
			System.out.println(this.first +" and "+ o.first);
			if(minus != 0){
				return minus;
			}
			System.out.println(this.second +" and2 "+ o.second);
			return (int)(o.second - this.second);
		}
		
	}
	
	//comparator to compare and sort the values
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
			int minus = (int) (this.first - o.first);
			if (minus != 0) {
				return minus;
			}
			return (int) (this.second - o.second);
		}
	}
}