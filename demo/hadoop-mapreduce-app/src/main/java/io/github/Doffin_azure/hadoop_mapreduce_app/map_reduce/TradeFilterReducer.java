package io.github.jiangdequan;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.w3c.dom.Text;

import java.io.IOException;

import javax.naming.Context;

public class TradeFilterReducer extends Reducer<Text, TradeData, Text, DoubleWritable> {
    private Text result = new Text();
    

    @Override
    protected void reduce(Text key, Iterable<TradeData> values, Context context) throws IOException, InterruptedException {
        int totalTradeQty = 0;
        double totalTradeAmount = 0;
        long timeStamp = 0;
        for (TradeData data : values) {
            totalTradeQty += data.getTradeQty();
            totalTradeAmount += data.getPrice() * data.getTradeQty();
            timeStamp = data.getTimeStamp();
        }
        // 计算成交额
        result.set(totalTradeQty+","+totalTradeAmount);

        // 输出到context
        context.write(key, result);
    }
}