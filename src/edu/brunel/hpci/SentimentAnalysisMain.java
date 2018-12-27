package edu.brunel.hpci;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysisMain {

	static HashMap<String, Integer> sentiments;
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		sentiments = new HashMap<String, Integer>();
		
		if (args.length != 3) {
			System.out.println("Need three input parameters. Dictionary path, tweet file path, output folder path");
			System.exit(-1);
		}
		
		sentiments = CreateSentimentMap(args[0]);

		Job job = Job.getInstance(conf, "Sentiment Analysis");
		job.setJarByClass(SentimentAnalysisMain.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(SentimentAnalysisMapper.class);
		job.setReducerClass(SentimentAnalysisReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

	private static HashMap<String, Integer> CreateSentimentMap(String URI) throws IOException {
		FileInputStream input = new FileInputStream(URI);
		BufferedReader br = new BufferedReader(new InputStreamReader(input, Charset.forName("UTF-8")));
		String eachLine, word;
		Integer score;
		String[] values;
		HashMap<String, Integer> sentiments = new HashMap<String, Integer>();
		while ((eachLine = br.readLine()) != null) {
			values = eachLine.split("\t");
			word = values[0];
			score = Integer.parseInt(values[1]);
			sentiments.put(word, score);
		}
		br.close();
		return sentiments;
	}
}
