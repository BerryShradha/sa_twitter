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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysisMain {

	static HashMap<String, Integer> sentimentWords;
	static HashMap<String, Integer> sentimentPhrases;
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		long startTime = System.currentTimeMillis();
		System.out.println("Start time: " + startTime);
		Configuration conf = new Configuration();
		sentimentWords = new HashMap<String, Integer>();
		sentimentPhrases = new HashMap<String, Integer>();
		
		if (args.length != 3) {
			System.out.println("Need three input parameters. Dictionary path, tweet file path, output folder path");
			System.exit(-1);
		}
		
		CreateSentimentMap(args[0]);
		Job job = Job.getInstance(conf, "Sentiment Analysis");
		job.setJarByClass(SentimentAnalysisMain.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(SentimentAnalysisMapper.class);
		job.setReducerClass(SentimentAnalysisReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if (job.waitForCompletion(true)) {
			long endTime = System.currentTimeMillis();
			System.out.println("End time: " + endTime);
			long timeDiff = endTime - startTime;
			System.out.println("Time taken: " + timeDiff);
			System.exit(0);
		}
		//System.exit(job.waitForCompletion(true)? 0 : 1);
	}

	private static void CreateSentimentMap(String URI) throws IOException {
		FileInputStream input = new FileInputStream(URI);
		BufferedReader br = new BufferedReader(new InputStreamReader(input, Charset.forName("UTF-8")));
		String eachLine, word;
		Integer score;
		String[] values;
		while ((eachLine = br.readLine()) != null) {
			values = eachLine.split("\t");
			word = values[0];
			score = Integer.parseInt(values[1]);
			if (word.contains(" ")) {
				sentimentPhrases.put(word,  score);
			}
			else {
				sentimentWords.put(word, score);
			}
		}
		br.close();
	}
}
