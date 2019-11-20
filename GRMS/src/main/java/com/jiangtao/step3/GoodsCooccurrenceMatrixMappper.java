package com.jiangtao.step3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GoodsCooccurrenceMatrixMappper extends Mapper<LongWritable, Text,Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String good = value.toString().split("\t")[0];

        context.write(new Text(good),value);
    }
}
