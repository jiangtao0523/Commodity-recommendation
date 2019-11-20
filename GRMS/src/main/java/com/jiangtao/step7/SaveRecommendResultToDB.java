package com.jiangtao.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SaveRecommendResultToDB extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new SaveRecommendResultToDB(),args));
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);

        job.setJarByClass(SaveRecommendResultToDB.class);

        DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.12.50/briup","root","root");

        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        job.setReducerClass(SaveRecommendResultToDBReducer.class);

        job.setMapOutputKeyClass(Result.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Result.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.addInputPath(job,new Path("output/step6"));
        DBOutputFormat.setOutput(job,"result","uid","gid","exp");

        return job.waitForCompletion(true)?0:1;
    }
}
