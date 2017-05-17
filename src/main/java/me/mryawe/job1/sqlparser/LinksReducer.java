package me.mryawe.job1.sqlparser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yawe on 16/05/17.
 */
public class LinksReducer extends Reducer<LongWritable, Text, Text, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pagerank = "1.0\t";

        boolean first = true;
        String pageName = "";

        for (Text value : values) {
            String val = value.toString();

            if(val.contains(":pn:")) {
                pageName = val.replaceAll(":pagename:", "");
            } else {
                if(!first) pagerank += ",";
                pagerank += val;
                first = false;
            }

        }

        context.write(new Text(pageName), new Text(pagerank));
    }
}