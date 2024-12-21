package mapper;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import util.*;


public class TradeFilterMapper extends Mapper<Object, Text, Text, TradeData> {
    private Text keyOut = new Text();
    private TradeData valueOut = new TradeData();
    private int aimSecurityID = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        aimSecurityID = context.getConfiguration().getInt("securityID", 1);
    }


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String tradeLine = value.toString();
        String[] columns = tradeLine.split("\t");

        int securityID = Integer.parseInt(columns[8]);
        long bidAppSeqNum = Long.parseLong(columns[10]);
        long offerAppSeqNum = Long.parseLong(columns[11]);
        double price = Double.parseDouble(columns[12]);
        int tradeQty = Integer.parseInt(columns[13]);
        String execType = columns[14];
        Long timeStamp = Long.parseLong(columns[15]);



        if ("F".equals(execType)) {
            if(securityID == aimSecurityID) {
            
            // 判断是买单还是卖单
            String orderType;
            if (bidAppSeqNum > offerAppSeqNum) {
                orderType = "Buy"; // 买单
                keyOut.set(orderType+"_"+securityID+"_"+bidAppSeqNum+"_"+timeStamp);
            } else {
                orderType = "Sell"; // 卖单 
                keyOut.set(orderType+"_"+securityID+"_"+offerAppSeqNum+"_"+timeStamp);
            }


            // 输出成交额
            valueOut = new TradeData(tradeQty, price);

            // 写入上下文
            context.write(keyOut, valueOut);

        }}
    }
}

