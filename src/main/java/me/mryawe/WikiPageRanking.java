package me.mryawe;

import me.mryawe.job1.sqlparser.LinksMapper;
import me.mryawe.job1.sqlparser.LinksReducer;
import me.mryawe.job1.sqlparser.PageMapper;
import me.mryawe.job2.calculate.RankCalculateMapper;
import me.mryawe.job2.calculate.RankCalculateReduce;
import me.mryawe.job2.calculate.PageRankJob;
import me.mryawe.job2.calculate.PageRankCounter;
import me.mryawe.job2.calculate.PageRankMapper;
import me.mryawe.job2.calculate.PageRankReducer;
import me.mryawe.job3.result.RankingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class WikiPageRanking extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WikiPageRanking(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        //boolean isCompleted = runSqlPageLinksParsingTest("input_links", "output_links");
        //if (!isCompleted) return 1;

        boolean isCompleted;
        File file;
        String job1Output = "temp_output/sql_parsing";
        String job2Output = "temp_output/pagerank_calculation";
        String job3Output = args[2];

        file = new File(job1Output);
        if (!file.exists()) {
            isCompleted = runSqlPageLinksParsing(args[0], args[1], job1Output);
            if (!isCompleted) return 1;
        }

        /*
        String lastResultPath = null;
        for (int runs = 0; runs < 5; runs++) {
            String inPath = "ranking/iter" + nf.format(runs);
            lastResultPath = "ranking/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }
        */

        file = new File(job2Output);
        if (!file.exists()) {
            isCompleted = runPageRank(job1Output, job2Output);
            if (!isCompleted) return 1;
        }

        file = new File(job3Output);
        if (!file.exists()) {
            isCompleted = runRankOrdering(job2Output, job3Output);
            if (!isCompleted) return 1;
        }

        return 0;
    }

    public boolean runSqlPageLinksParsing(String pageSqlInputPath, String linksSqlInputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job sqlParser = Job.getInstance(conf, "sqlParserLinks");
        sqlParser.setJarByClass(WikiPageRanking.class);

        sqlParser.setOutputKeyClass(LongWritable.class);
        sqlParser.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(sqlParser, new Path(pageSqlInputPath),TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(sqlParser, new Path(linksSqlInputPath),TextInputFormat.class, LinksMapper.class);
        FileOutputFormat.setOutputPath(sqlParser, new Path(outputPath));
        sqlParser.setReducerClass(LinksReducer.class);


        return sqlParser.waitForCompletion(true);
    }

    public boolean runSqlPageLinksParsingTest(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job sqlParser = Job.getInstance(conf, "sqlParserLinks");
        sqlParser.setJarByClass(WikiPageRanking.class);

        sqlParser.setOutputKeyClass(LongWritable.class);
        sqlParser.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(sqlParser, new Path(inputPath));
        FileOutputFormat.setOutputPath(sqlParser, new Path(outputPath));
        sqlParser.setMapperClass(LinksMapper.class);
        sqlParser.setNumReduceTasks(0);
        //sqlParser.setReducerClass(LinksReducer.class);


        return sqlParser.waitForCompletion(true);
    }

    /*
    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(WikiPageRanking.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReduce.class);

        return rankCalculator.waitForCompletion(true);
    }
    */

    private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(WikiPageRanking.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }
    private boolean runPageRank(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        long i = 0;
        Path input = new Path(inputPath);
        String root = input.getParent().toString().concat("/");
        Path output = new Path(root.concat(String.valueOf(i)));
        if (PageRankJob.run(input, output, false)) {
            do {
                input = new Path(root.concat(String.valueOf(i)));
                output = new Path(root.concat(String.valueOf(i + 1)));
                i++;
            } while (PageRankJob.run(input, output, false));
        }
        input = new Path(root.concat(String.valueOf(i)));
        output = new Path(outputPath);
        PageRankJob.run(input, output, false);

        return true;
    }
}
