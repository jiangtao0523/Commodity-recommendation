package com.jiangtao.step6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DuplicateDataForResultFirstMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Set<String> set = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        BufferedReader br = new BufferedReader(new FileReader("matrix.txt"));
        String line = null;
        while((line = br.readLine()) != null){
            String[] fields = line.split(" ");
            set.add(fields[0] + "," + fields[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        if(!set.contains(fields[0])){
            String[] userGood = fields[0].split(",");
            context.write(new Text(userGood[0] + "\t" + userGood[1]),new IntWritable(Integer.parseInt(fields[1])));
        }
    }
}
