/*
 * File name   : WordCount.java
 * Functions   : Performs the map reduce job on the input set - bible
 * Author      : Noopur R K
 * UFID        : 1980 - 9834
 * Institution : University of Florida 
 */

package com.wordcount3;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;


public class WordCount{


	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		Set<String> WordCountList;
		StringTokenizer Inputlines;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			Inputlines = new StringTokenizer(value.toString());
			String nextToken = "";
			while (Inputlines.hasMoreTokens()) {
				//get next token
				nextToken = Inputlines.nextToken();

				//generate key,value pair if token exists in the list of words from cached file
				if (WordCountList.contains(nextToken)) 
				{
					word.set(nextToken);
					context.write(word, one);
				}
			}			
		}

		@Override
		protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {		
			String cached_words = FileUtils.readFileToString(new File("./PatternFile"));
			String[] cached_word_list = cached_words.split(" ");
			WordCountList = new HashSet<String>(Arrays.asList(cached_word_list));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 

				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount3");
		job.addCacheFile(new URI("s3://biblewordcount/word-patterns.txt" + "#PatternFile"));

		job.setJarByClass(WordCount.class);
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(Reduce.class);    

		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(4);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);                                                                                                                                                                 
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}