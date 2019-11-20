package com.jiangtao.step6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class DuplicateDataForResult extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new DuplicateDataForResult(),args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);

        job.setJarByClass(DuplicateDataForResult.class);

        job.setMapperClass(DuplicateDataForResultFirstMapper.class);
        job.setReducerClass(DuplicateDataForResultReducer.class);

        job.addCacheFile(new URI("hdfs://test1:8020/user/jiangtao/input/matrix.txt"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,new Path("output/step5"));
        TextOutputFormat.setOutputPath(job,new Path("output/step6"));

        return job.waitForCompletion(true)?0:1;
    }
}
