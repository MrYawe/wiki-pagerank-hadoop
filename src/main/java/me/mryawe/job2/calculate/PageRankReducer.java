/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.mryawe.job2.calculate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jihad
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final double EPSILON = 0.00001;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	double sum = 0;
	double oldPageRank = 0;
	String[] links = null;
	int position = 0;
	for (Text value : values) {
	    position = value.find("\t");
	    if (position == -1) {
		if (value.toString().contains("#")) {
		    sum += Double.parseDouble(value.toString().substring(1)); 
		} else {
		    oldPageRank = Double.parseDouble(value.toString());
		}
	    } else {
		oldPageRank = Double.parseDouble(Text.decode(value.getBytes(), 0, position));
		links = Text.decode(value.getBytes(), position + 1, value.getLength() - (position + 1)).split("\t");
	    }
	}
	double newPageRank = 0.15 + 0.85 * sum;
	if (Math.abs(newPageRank - oldPageRank) < EPSILON) {
	    context.getCounter(PageRankCounter.STABLE).increment(1);
	} else {
	    context.getCounter(PageRankCounter.UNSTABLE).increment(1);
	}
	String value = new String();
	if (links != null) {
	    for (String link : links) {
		value = value.concat("\t".concat(link));
	    }
	}
	value = String.valueOf(newPageRank).concat(value);
	context.write(key, new Text(value));
    }

}
