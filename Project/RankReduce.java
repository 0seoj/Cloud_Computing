package mju.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
	private Text value = new Text();

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int rank = Integer.parseInt(conf.get("rank"));
		
		//result of top5
		if(rank<5) {
			for(Text text : values){	
				value.set(text);
			}
			context.write(value, key);
		}
		rank += 1;
		
		conf.set("rank", String.valueOf(rank));
	}
}