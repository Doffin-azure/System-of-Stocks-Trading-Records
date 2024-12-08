package io.github.jiangdequan;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class TradeMergeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    private LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long totalTradeAmount = 0;

        // 遍历相同key的所有value，计算总和
        for (LongWritable value : values) {
            
            totalTradeAmount += value.get();
        }

        // 将结果设置到result对象中
        result.set(totalTradeAmount);

        // 输出结果到context
        context.write(key, result);
    }
}
