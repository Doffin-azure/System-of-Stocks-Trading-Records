package reducer;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TradeMergeReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double totalTradeAmount = 0.0;
        Long totalTradeQty = 0L;

        // 遍历相同key的所有value，计算总和
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            totalTradeAmount += Double.parseDouble(parts[1]);
            totalTradeQty += Long.parseLong(parts[0]);
        }

        // 将结果设置到result对象中
        result.set(totalTradeQty + "," + totalTradeAmount);

        // 输出结果到context
        context.write(key, result);
    }
}