package edu.brunel.hpci;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysisMain {

	static HashMap<String, Integer> sentiments;
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		sentiments = new HashMap<String, Integer>();
		
		if (args.length != 4) {
			System.out.println("Mismatch in number of input parameters");
			System.exit(-1);
		}
		
		conf.set("FilterValue", args[3]);
		Job job = Job.getInstance(conf, "Filter Temperature");
		job.setJarByClass(SentimentAnalysisMain.class);
		
		String word;
		Integer score;
				
		Path dictionaryPath  = new Path(args[0]);
		Scanner scanner = new Scanner((Readable) dictionaryPath);
		while (scanner.hasNextLine()) {
			String lines = scanner.nextLine();
			String[] line = lines.split("\t");
			 word = line[0];
			 score = Integer.parseInt(line[1]);	
			 sentiments.put(word, score);
		}
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(SentimentAnalysisMapper.class);
		job.setReducerClass(SentimentAnalysisReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);

	}

}
