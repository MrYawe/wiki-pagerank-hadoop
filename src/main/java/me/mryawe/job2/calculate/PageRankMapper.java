/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.mryawe.job2.calculate;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jihad
 */
public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws CharacterCodingException, IOException, InterruptedException {
		int position = value.find("\t");
		if (position != -1) {
		    double oldPageRank = Double.parseDouble(Text.decode(value.getBytes(), 0, position));
		    String[] links = Text.decode(value.getBytes(), position + 1, value.getLength() - (position + 1)).split(",");
		    for (String link : links) {
			context.write(new Text(link), new Text("#".concat(String.valueOf(oldPageRank / links.length))));
		    }
		}
		context.write(key, value);
	}
}
