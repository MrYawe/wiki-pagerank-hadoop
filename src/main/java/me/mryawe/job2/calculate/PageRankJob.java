/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.mryawe.job2.calculate;

import me.mryawe.WikiPageRanking;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author jihad
 */
public class PageRankJob {
	
    private static final double RATIO = 0.90;
	
    public static boolean run(Path input, Path output, boolean deleteInput) throws IOException, InterruptedException, ClassNotFoundException {
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(configuration);
		
	if (!fs.exists(input))
	    throw new IOException("The system cannot find the file specified");
		
	if (fs.exists(output))
	    fs.delete(output, true);
		
	Job job = Job.getInstance(configuration, "pageRankCalculator");
	job.setJarByClass(WikiPageRanking.class);
	job.setMapperClass(PageRankMapper.class);
	job.setReducerClass(PageRankReducer.class);
	job.setInputFormatClass(KeyValueTextInputFormat.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, input);
	FileOutputFormat.setOutputPath(job, output);
	if (!job.waitForCompletion(true)) {
	    System.exit(1);
	}

	if (deleteInput)
	    fs.delete(input, true);
		
	long stable = job.getCounters().findCounter(PageRankCounter.STABLE).getValue();
	long unstable = job.getCounters().findCounter(PageRankCounter.UNSTABLE).getValue();
	double ratio = (double) stable / (double) (unstable + stable);
	return !(ratio > RATIO);
    }
}
