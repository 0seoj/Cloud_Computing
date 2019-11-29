package mju.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text keyData = new Text();
	private IntWritable valueData = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String check = conf.get("check");
	
		String line = value.toString();
		String[] values = line.split(",");
		String name = values[0];
		String category = values[1];
		int review = Integer.parseInt(values[3]);
		int install = Integer.parseInt(values[5].substring(0,values[5].length()-1));
		
		//make key value pair
		if(category.equals(check)) {
			keyData.set(name);
			valueData.set(review+install);
			context.write(keyData, valueData);
		}
	}
}