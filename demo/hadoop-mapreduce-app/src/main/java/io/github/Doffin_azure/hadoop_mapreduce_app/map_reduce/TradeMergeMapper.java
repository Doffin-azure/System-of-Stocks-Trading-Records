package io.github.jiangdequan;

import java.io.IOException;

import javax.naming.Context;

import org.w3c.dom.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TradeMergeMapper extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {
    Long allStock = 17170245800L;
    int k = 1;// 用于计算时间窗口
    private Text keyOut = new Text();
    private LongWritable valueOut = new LongWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 直接将从上一个Reducer输出的结果传递给Reducer
        String[] columns = key.toString().split("_");
        Long timeStamp = Long.parseLong(columns[3]);
        String orderType = columns[0];
        String securityID = columns[1];
        String appSeqNum = columns[2];

        String[] values = value.toString().split(",");

        Long tradeQty = Long.parseLong(values[0]);
        Long tradeAmount = Long.parseLong(values[1]);

        String tradeType = "";
        // 超大单
        if (tradeQty >= 200000 || tradeAmount >= 1000000 || (double)tradeQty / allStock >= 0.003) {
            tradeType = "ExtraLarge";
        }
        // 大单
        else if (tradeQty >= 60000 || tradeAmount >= 300000 || (double)tradeQty / allStock >= 0.001) {
            tradeType = "Large";
        }
        // 中单
        else if (tradeQty >= 10000 || tradeAmount >= 50000 || (double)tradeQty / allStock >= 0.00017) {
            tradeType = "Medium";
        } else {
            tradeType = "Small";
        }

        // 时间窗口

        String tradingTimeSegment = getTradingTimeSegment(timeStamp);
        keyOut.set(orderType + "_" + securityID + "_" + tradingTimeSegment + "_" + tradeType);

        valueOut.set(tradeAmount);

        context.write(keyOut, valueOut);
    }

    String getTradingTimeSegment(Long timeStamp) {
        Date date = new Date(timeStamp);
        SimpleDateFormat sdf = new SimpleDateFormat("HHmmssSSS");
        String timeStr = hourMinFormat.format(date);
        // 判断是否在开盘集合竞价时间段 (09:15 - 09:25)
        if (timeStr.compareTo("09:15") >= 0 && timeStr.compareTo("09:25") <= 0) {
            return "OpenAuction";
        }
        // 判断是否在上午连续竞价时间段 (09:30 - 11:30)
        else if (timeStr.compareTo("09:30") >= 0 && timeStr.compareTo("11:30") <= 0) {
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            int min = Integer.parseInt(timeStr.substring(2, 4));
            int totalMinutes = (hour - 9) * 60 + min;
            int segment = totalMinutes / k + 1;
            return "MorningAuction" + segment;

        }
        // 判断是否在下午连续竞价时间段 (13:00 - 15:00)
        else if (timeStr.compareTo("13:00") >= 0 && timeStr.compareTo("15:00") <= 0) {
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            int min = Integer.parseInt(timeStr.substring(2, 4));
            int totalMinutes = (hour - 13) * 60 + min;
            int segment = totalMinutes / k + 1;
            return "AfternoonAuction" + segment;
        }
        // 判断是否在收盘集合竞价时间段 (14:57 - 15:00)
        else if (timeStr.compareTo("14:57") >= 0 && timeStr.compareTo("15:00") <= 0) {
            return "CloseAuction";
        }
        return "UnknownSegment"; // 如果时间戳不在任何预定义的时间段内

    }
}