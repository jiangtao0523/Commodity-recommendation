package com.jiangtao.step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserBuyGoodsListMapper extends Mapper<LongWritable, Text, IntWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split(" ");

        context.write(new IntWritable(Integer.parseInt(fields[0])),new Text(fields[1]));
    }
}
