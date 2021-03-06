package com.simplilearn.bigdata.casestudy_4.solution_2_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class Solution {

    public static void main(String[] args) throws Exception {

        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Airlines names canceled");

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setMapperClass(FlightMapper.class);
            job.setReducerClass(FlightReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(Solution.class);

            for(int i =0 ;i < args.length ;i ++) {
                System.out.println("arg "+args[i]);
            }
            if (args.length == 3) {
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.addCacheFile(new URI(args[2]));
            } else {
                System.out.print("Please provide input and output path and cached file path.");
                System.exit(0);
            }
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
