package com.jiangtao.step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserBuyGoodsListReducer extends Reducer<IntWritable, Text,IntWritable,Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for(Text value : values){
            sb.append(value.toString() + ",");
        }
        sb.setLength(sb.length() - 1);

        context.write(key,new Text(sb.toString()));
    }
}
