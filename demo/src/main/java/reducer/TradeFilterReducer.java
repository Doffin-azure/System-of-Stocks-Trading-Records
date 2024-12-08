package reducer;

import util.*;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TradeFilterReducer extends Reducer<Text, TradeData, Text, Text> {
    private Text result = new Text();
    

    @Override
    protected void reduce(Text key, Iterable<TradeData> values, Context context) throws IOException, InterruptedException {
        int totalTradeQty = 0;
        double totalTradeAmount = 0;
        for (TradeData data : values) {
            totalTradeQty += data.getTradeQty();
            totalTradeAmount += data.getPrice() * data.getTradeQty();
        }
        // 计算成交额
        result.set(totalTradeQty+","+totalTradeAmount);

        // 输出到context
        context.write(key, result);
    }
}