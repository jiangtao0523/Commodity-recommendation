package com.jiangtao.step3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GoodsCooccurrenceMatrixReducer extends Reducer<Text, Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Map<String,Integer> map = new HashMap<>();
        Text v = new Text();

        for(Text value : values){
            String good = value.toString().split("\t")[1];
            if(map.containsKey(good)){
                map.put(good,(map.get(good) + 1));
            }else {
                map.put(good, 1);
            }
        }

        Set<Map.Entry<String, Integer>> entries = map.entrySet();
        StringBuffer sb = new StringBuffer();
        for(Map.Entry<String, Integer> entry : entries){
            sb.append(entry.getKey() + ":" + entry.getValue() + ",");
        }
        sb.setLength(sb.length() - 1);

        v = new Text(sb.toString());
        context.write(key,v);
    }
}
