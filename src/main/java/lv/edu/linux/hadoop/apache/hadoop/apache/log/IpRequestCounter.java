package lv.edu.linux.hadoop.apache.hadoop.apache.log;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class IpRequestCounter {

	public static class IpRequestCounterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text ip = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();

			int pos = line.indexOf(" ");
			ip.set(line.substring(0, pos));
			output.collect(ip, one);
		}
	}

	public static class IpRequestCounterReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
}
