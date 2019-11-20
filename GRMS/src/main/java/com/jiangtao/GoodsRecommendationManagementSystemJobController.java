package com.jiangtao;

import com.jiangtao.step1.UserBuyGoodsList;
import com.jiangtao.step1.UserBuyGoodsListMapper;
import com.jiangtao.step1.UserBuyGoodsListReducer;
import com.jiangtao.step2.GoodsCooccurrenceList;
import com.jiangtao.step2.GoodsCooccurrenceListMapper;
import com.jiangtao.step2.GoodsCooccurrenceListReducer;
import com.jiangtao.step3.GoodsCooccurrenceMatrix;
import com.jiangtao.step3.GoodsCooccurrenceMatrixMappper;
import com.jiangtao.step3.GoodsCooccurrenceMatrixReducer;
import com.jiangtao.step4.UserBuyGoodsVector;
import com.jiangtao.step4.UserBuyGoodsVectorMapper;
import com.jiangtao.step4.UserBuyGoodsVectorReducer;
import com.jiangtao.step5.MultiplyGoodsMatrixAndUserVector;
import com.jiangtao.step5.MultiplyGoodsMatrixAndUserVectorMapper;
import com.jiangtao.step5.MultiplyGoodsMatrixAndUserVectorReducer;
import com.jiangtao.step6.DuplicateDataForResult;
import com.jiangtao.step6.DuplicateDataForResultFirstMapper;
import com.jiangtao.step6.DuplicateDataForResultReducer;
import com.jiangtao.step7.Result;
import com.jiangtao.step7.SaveRecommendResultToDB;
import com.jiangtao.step7.SaveRecommendResultToDBMapper;
import com.jiangtao.step7.SaveRecommendResultToDBReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class GoodsRecommendationManagementSystemJobController extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        //step1
        Job job1 = Job.getInstance(conf);

        job1.setJarByClass(UserBuyGoodsList.class);

        job1.setMapperClass(UserBuyGoodsListMapper.class);
        job1.setReducerClass(UserBuyGoodsListReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job1,new Path("input/matrix.txt"));
        TextOutputFormat.setOutputPath(job1,new Path("output/step1"));

        //step2
        Job job2 = Job.getInstance(conf);

        job2.setJarByClass(GoodsCooccurrenceList.class);

        job2.setMapperClass(GoodsCooccurrenceListMapper.class);
        job2.setReducerClass(GoodsCooccurrenceListReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job2,new Path("output/step1"));
        TextOutputFormat.setOutputPath(job2,new Path("output/step2"));

        //step3
        Job job3 = Job.getInstance(conf);

        job3.setJarByClass(GoodsCooccurrenceMatrix.class);

        job3.setMapperClass(GoodsCooccurrenceMatrixMappper.class);
        job3.setReducerClass(GoodsCooccurrenceMatrixReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job3,new Path("output/step2"));
        TextOutputFormat.setOutputPath(job3,new Path("output/step3"));

        //step4
        Job job4 = Job.getInstance(conf);

        job4.setJarByClass(UserBuyGoodsVector.class);

        job4.setMapperClass(UserBuyGoodsVectorMapper.class);
        job4.setReducerClass(UserBuyGoodsVectorReducer.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job4,new Path("input/matrix.txt"));
        TextOutputFormat.setOutputPath(job4,new Path("output/step4"));

        //step5
        Job job5 = Job.getInstance(conf);

        job5.setJarByClass(MultiplyGoodsMatrixAndUserVector.class);

        job5.setMapperClass(MultiplyGoodsMatrixAndUserVectorMapper.class);
        job5.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);

        job5.addCacheFile(new URI("hdfs://test1:8020/user/jiangtao/output/step4/part-r-00000"));

        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(IntWritable.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);

        job5.setInputFormatClass(TextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job5,new Path("output/step3"));
        TextOutputFormat.setOutputPath(job5,new Path("output/step5"));

        //step6
        Job job6 = Job.getInstance(conf);

        job6.setJarByClass(DuplicateDataForResult.class);

        job6.setMapperClass(DuplicateDataForResultFirstMapper.class);
        job6.setReducerClass(DuplicateDataForResultReducer.class);

        job6.addCacheFile(new URI("hdfs://test1:8020/user/jiangtao/input/matrix.txt"));

        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);

        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);

        job6.setInputFormatClass(TextInputFormat.class);
        job6.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job6,new Path("output/step5"));
        TextOutputFormat.setOutputPath(job6,new Path("output/step6"));

        //step7
        Job job7 = Job.getInstance(conf);

        job7.setJarByClass(SaveRecommendResultToDB.class);

        DBConfiguration.configureDB(job7.getConfiguration(),"com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.12.50/briup","root","root");

        job7.setMapperClass(SaveRecommendResultToDBMapper.class);
        job7.setReducerClass(SaveRecommendResultToDBReducer.class);

        job7.setMapOutputKeyClass(Result.class);
        job7.setMapOutputValueClass(NullWritable.class);

        job7.setOutputKeyClass(Result.class);
        job7.setOutputValueClass(NullWritable.class);

        job7.setInputFormatClass(TextInputFormat.class);
        job7.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.addInputPath(job7,new Path("output/step6"));
        DBOutputFormat.setOutput(job7,"result","uid","gid","exp");

        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);
        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);
        ControlledJob cj3 = new ControlledJob(conf);
        cj3.setJob(job3);
        ControlledJob cj4 = new ControlledJob(conf);
        cj4.setJob(job4);
        ControlledJob cj5 = new ControlledJob(conf);
        cj5.setJob(job5);
        ControlledJob cj6 = new ControlledJob(conf);
        cj6.setJob(job6);
        ControlledJob cj7 = new ControlledJob(conf);
        cj7.setJob(job7);

        cj2.addDependingJob(cj1);
        cj3.addDependingJob(cj2);
        cj4.addDependingJob(cj3);
        cj5.addDependingJob(cj4);
        cj6.addDependingJob(cj5);
        cj7.addDependingJob(cj6);

        JobControl jc = new JobControl("GoodsRecommend.");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);

        new Thread(jc).start();

        while(true){
            for(ControlledJob c:jc.getRunningJobList()){
                c.getJob().monitorAndPrintJob();
            }
            if(jc.allFinished())break;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new GoodsRecommendationManagementSystemJobController(),args));
    }
}
