package me.mryawe.job1.sqlparser;

/**
 * Created by yawe on 16/05/17.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PageMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final Pattern wikiPagePattern = Pattern.compile("\\((\\d*),0,'([^,]*?)','.*?\\)+[,|;]");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = wikiPagePattern.matcher(value.toString());

        while (matcher.find()) {
            Long pageId = Long.parseLong(matcher.group(1));
            String pageName = matcher.group(2);

            context.write(new LongWritable(pageId), new Text(":pn:"+pageName));
        }
    }
}
