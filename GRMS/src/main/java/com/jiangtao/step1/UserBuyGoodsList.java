package com.jiangtao.step1;

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

public class UserBuyGoodsList extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new UserBuyGoodsList(),args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);

        job.setJarByClass(UserBuyGoodsList.class);

        job.setMapperClass(UserBuyGoodsListMapper.class);
        job.setReducerClass(UserBuyGoodsListReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,new Path("input/matrix.txt"));
        TextOutputFormat.setOutputPath(job,new Path("output/step1"));

        return job.waitForCompletion(true)?0:1;
    }


}
