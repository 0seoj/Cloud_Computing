package mju.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		if(args.length == 3){
			conf.set("check", args[2]); //category set
		}else{
			System.out.println("use category please");
			return;
		}
		
		//set job
		Job first = new Job(conf,"filter");
		first.setJarByClass(Project.class);
		
		//set datatype of job output
		first.setOutputKeyClass(Text.class);
		first.setOutputValueClass(IntWritable.class);
		
		//set mapper, reducer
		first.setMapperClass(Map.class);
		first.setReducerClass(Reduce.class);
		
		first.setInputFormatClass(TextInputFormat.class);
		first.setOutputFormatClass(TextOutputFormat.class);
		
		//number of reducetask
		first.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(first, new Path(args[0]));
		FileOutputFormat.setOutputPath(first, new Path(args[1]));
		
		boolean success = first.waitForCompletion(true);
		System.out.println(success);
		
		
		String input = args[1] + "/part-r-00000";
		String output = args[1] + "/Rank";

		conf.set("rank", "0");
		
		Job second = new Job(conf, "sort");
		second.setJarByClass(Project.class);

		second.setMapOutputKeyClass(IntWritable.class);
		second.setMapOutputValueClass(Text.class);

		second.setOutputKeyClass(Text.class);
		second.setOutputValueClass(IntWritable.class);

		second.setMapperClass(RankMap.class);
		second.setReducerClass(RankReduce.class);
		second.setSortComparatorClass(RankCompare.class);

		second.setInputFormatClass(TextInputFormat.class);
		second.setOutputFormatClass(TextOutputFormat.class);

		second.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(second, new Path(input));
		FileOutputFormat.setOutputPath(second, new Path(output));

		success = second.waitForCompletion(true);
		System.out.println(success);
		
	}
}