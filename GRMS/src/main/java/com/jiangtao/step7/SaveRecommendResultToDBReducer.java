package com.jiangtao.step7;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaveRecommendResultToDBReducer extends Reducer<Result, NullWritable, Result,NullWritable> {

    @Override
    protected void reduce(Result key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key,NullWritable.get());
    }
}
