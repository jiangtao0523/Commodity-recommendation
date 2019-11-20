package com.jiangtao.step7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SaveRecommendResultToDBMapper extends Mapper<LongWritable, Text,Result, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Result result = new Result();
        String[] fields = value.toString().split("\t");
        result.setUid(fields[0]);
        result.setGid(fields[1]);
        result.setExp(Integer.parseInt(fields[2]));

        context.write(result,NullWritable.get());
    }
}
