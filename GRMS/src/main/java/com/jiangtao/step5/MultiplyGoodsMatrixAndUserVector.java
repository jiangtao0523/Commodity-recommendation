package com.jiangtao.step5;

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

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new MultiplyGoodsMatrixAndUserVector(),args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MultiplyGoodsMatrixAndUserVector.class);

        job.setMapperClass(MultiplyGoodsMatrixAndUserVectorMapper.class);
        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);

        job.addCacheFile(new URI("hdfs://test1:8020/user/jiangtao/output/step4/part-r-00000"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,new Path("output/step3"));
        TextOutputFormat.setOutputPath(job,new Path("output/step5"));

        return job.waitForCompletion(true)?0:1;
    }
}
