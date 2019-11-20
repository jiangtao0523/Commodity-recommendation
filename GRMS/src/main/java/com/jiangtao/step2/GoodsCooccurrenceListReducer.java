package com.jiangtao.step2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GoodsCooccurrenceListReducer extends Reducer<Text, NullWritable,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String[] goods = key.toString().split(",");

        for(int i = 0; i < goods.length; i++){
            for(int j = 0; j < goods.length; j++){
                context.write(new Text(goods[i]),new Text(goods[j]));
            }
        }
    }

}
