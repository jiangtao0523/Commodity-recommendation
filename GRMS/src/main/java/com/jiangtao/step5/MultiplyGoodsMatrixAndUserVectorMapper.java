package com.jiangtao.step5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MultiplyGoodsMatrixAndUserVectorMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Map<String,String> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new FileReader("part-r-00000"));
        String line;
        while((line = br.readLine()) != null){
            String[] fields = line.split("\t");
            map.put(fields[0],fields[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        String[] concurrencyCounts = fields[1].split(",");
        String[] buyVectors = map.get(fields[0]).split(",");

        for (String buyVector : buyVectors){
            String user = buyVector.split(":")[0];
            String vector = buyVector.split(":")[1];

            for(String concurrencyCount : concurrencyCounts){
                String good = concurrencyCount.split(":")[0];
                String count = concurrencyCount.split(":")[1];

                context.write(new Text(user + "," + good),
                        new IntWritable((Integer.parseInt(vector) * Integer.parseInt(count))));
            }
        }


    }
}
