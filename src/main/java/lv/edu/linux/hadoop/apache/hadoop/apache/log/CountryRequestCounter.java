package lv.edu.linux.hadoop.apache.hadoop.apache.log;

import java.io.File;
import java.io.IOException;
import java.util.*;
import lv.edu.linux.hadoop.apache.hadoop.apache.log.GeoIPDb.GeoIpRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class CountryRequestCounter {

	public static class CountryRequestCounterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private IntWritable country_count = new IntWritable(1);
		private Text country = new Text();
		private static GeoIPDb geoipdb;

		
		@Override
		public void configure(JobConf jobConf) {
			String geoip_db_location = jobConf.get("geoipdb");
			geoipdb = new GeoIPDb(geoip_db_location);
			super.configure(jobConf);
		}
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();

			int pos = line.indexOf("\t");
			String ip = line.substring(0, pos);
			int count = Integer.decode(line.substring(pos + 1));
			country_count.set(count);
			country.set(geoipdb.ipToCountry(ip));
			output.collect(country, country_count);
		}
	}

	public static class CountryRequestCounterReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		conf.setJobName("DomainFinder");

		// This line specifies the jar Hadoop should use to run the mapper and
		// reducer by telling it a class that’s inside it
		conf.setJarByClass(CountryRequestCounter.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CountryRequestCounterMapper.class);
		conf.setReducerClass(CountryRequestCounterReducer.class);

		// KeyValueTextInputFormat treats each line as an input record,
		// and splits the line by the tab character to separate it into key and value
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Location of geoip db on local filesystem for local runs
		conf.set("geoipdb", "file:///home/martins/hadoop/data/GeoIPCountryWhois.csv");

		// Run this job locally
		conf.set("mapreduce.jobtracker.address", "local");
		conf.set("fs.defaultFS", "file:///");

		String output_dir = "/home/martins/hadoop/output/";
		deleteLocalDir(new File(output_dir));

		FileInputFormat.setInputPaths(conf, new Path("/home/martins/hadoop/data/apache-logs-ip-list/"));
		FileOutputFormat.setOutputPath(conf, new Path(output_dir));

		JobClient.runJob(conf);
	}

	private static boolean deleteLocalDir(File directory) {
		if (directory.exists()) {
			File[] files = directory.listFiles();
			if (null != files) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						deleteLocalDir(files[i]);
					} else {
						files[i].delete();
					}
				}
			}
		}
		return (directory.delete());
	}
}
