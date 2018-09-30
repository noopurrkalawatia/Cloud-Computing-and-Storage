/*
 * File name   : WordCount.java
 * Functions   : Performs the map reduce job on the input set taking 2 words as a key - bible
 * Author      : Noopur R K
 * UFID        : 1980 - 9834
 * Institution : University of Florida 
 */

package com.wordcount2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String previousWord = "";
            String currentWord = "";
	
	        // get first line
	
	        StringTokenizer lineList = new StringTokenizer(value.toString());
	        if (lineList.hasMoreTokens()) 
            {
                // get last word
	        	previousWord = lineList.nextToken().toLowerCase();
	        	previousWord.replaceAll("[^A-Za-z]+", "");
	        }
	        
	        while (lineList.hasMoreTokens()) 
            {
	        	currentWord = lineList.nextToken().toLowerCase();
	        	currentWord.replaceAll("[^A-Za-z]+", "");
	        	if (currentWord.isEmpty()) {
					continue;
				}
		        // get the key which is previousWord+space+currentWord
		        word.set(new Text(previousWord + " " + currentWord));
		        context.write(word,one);
		        previousWord = currentWord; 
			}
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
		Job job = Job.getInstance(conf, "wordcount2");
		
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
