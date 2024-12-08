package reducer;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TradeMergeReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double totalTradeAmount = 0.0;

        // 遍历相同key的所有value，计算总和
        for (DoubleWritable value : values) {
            
            totalTradeAmount += value.get();
        }

        // 将结果设置到result对象中
        result.set(totalTradeAmount.toString());

        // 输出结果到context
        context.write(key, result);
    }
}