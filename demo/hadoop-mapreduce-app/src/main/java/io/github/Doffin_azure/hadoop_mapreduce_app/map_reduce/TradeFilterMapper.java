package io.github.jiangdequan;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.w3c.dom.Text;

import java.io.IOException;

import javax.naming.Context;

public class TradeFilterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text keyOut = new Text();
    private TradeData valueOut = new TradeData();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String tradeLine = value.toString();
        String[] columns = tradeLine.split("\t");

        int securityID = Integer.parseInt(columns[8]);
        long bidAppSeqNum = Long.parseLong(columns[10]);
        long offerAppSeqNum = Long.parseLong(columns[11]);
        double price = Double.parseDouble(columns[12]);
        int tradeQty = Integer.parseInt(columns[13]);
        String execType = columns[14];

        if ("F".equals(execType)) {
            // 判断是买单还是卖单
            String orderType;
            if (bidAppSeqNum > offerAppSeqNum) {
                orderType = "Buy"; // 买单
                keyOut.set(orderType+"_"+securityID+"_"+bidAppSeqNum);
            } else {
                orderType = "Sell"; // 卖单 
                keyOut.set(orderType+"_"+securityID+"_"+offerAppSeqNum);
            }


            // 输出成交额
            valueOut = new TradeData(tradeQty, price); 

            // 写入上下文
            context.write(keyOut, valueOut);

        }
    }
}

