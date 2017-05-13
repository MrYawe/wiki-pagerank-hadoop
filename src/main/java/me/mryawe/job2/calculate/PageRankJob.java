package me.mryawe.job2.calculate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import me.mryawe.WikiPageRanking;

/**
 *
 * @author jihad
 */
public class PageRankJob {
	
	private static final double RATIO = 0.90;
	
	public static boolean run(Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		if (!fs.exists(input))
			throw new IOException("The system cannot find the file specified");
		
		if (fs.exists(output))
			fs.delete(output, true);
		
		Job job = Job.getInstance(configuration);
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
		
		long stable = job.getCounters().findCounter(PageRankCounter.STABLE).getValue();
		long unstable = job.getCounters().findCounter(PageRankCounter.UNSTABLE).getValue();
		double ratio = (double) stable / (double) (unstable + stable);
		System.out.printf("Stable = %d, Unstable = %d, ratio = %f, stop = %b\n", stable, unstable, ratio, ratio > RATIO);
		return !(ratio > RATIO);
	}
}
