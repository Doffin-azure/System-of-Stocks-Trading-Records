package mapper;

import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TradeMergeMapper extends Mapper<Object, Text, Text, Text> {
    Long allStock = 17170245800L;
    int k = 20;// 用于计算时间窗口
    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 直接将从上一个Reducer输出的结果传递给Reducer

        String line = value.toString();
        String[] parts = line.split("\t");
        key = new Text(parts[0]);
        value = new Text(parts[1]);
        String[] columns = key.toString().split("_");
        String timeStamp = columns[3];
        String orderType = columns[0];
        String securityId = columns[1];// format: long 

        String[] values = value.toString().split(",");

        Long tradeQty = Long.parseLong(values[0]);
        Double tradeAmount = Double.parseDouble(values[1]);

        String tradeType = "";
        // 超大单
        if (tradeQty >= 200000 || tradeAmount >= 1000000 || (double) tradeQty / allStock >= 0.003) {
            tradeType = "ExtraLarge";
        }
        // 大单
        else if (tradeQty >= 60000 || tradeAmount >= 300000 || (double) tradeQty / allStock >= 0.001) {
            tradeType = "Large";
        }
        // 中单
        else if (tradeQty >= 10000 || tradeAmount >= 50000 || (double) tradeQty / allStock >= 0.00017) {
            tradeType = "Medium";
        } else {
            tradeType = "Small";
        }

        // 时间窗口

        String tradingTimeSegment;
        try {
            tradingTimeSegment = getTradingTimeSegment(timeStamp);
            if (tradingTimeSegment.equals("skip") ){
                return;
            }
            keyOut.set(orderType + "_" + securityId + "_" + tradingTimeSegment + "_" + tradeType);

            valueOut.set(tradeQty + "," + tradeAmount);

            context.write(keyOut, valueOut);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    String getTradingTimeSegment(String timeStamp) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        Date date = sdf.parse(timeStamp);

        // 使用新的 SimpleDateFormat 格式化输出
        SimpleDateFormat outputFormat = new SimpleDateFormat("HHmm");
        String timeStr = outputFormat.format(date);

        // 获取时间戳的日期部分
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String datePart = dateFormat.format(date);

        // 判断是否在开盘集合竞价时间段 (09:15 - 09:25)
        if (timeStr.compareTo("0915") >= 0 && timeStr.compareTo("0925") <= 0) {
            String startTime = datePart + "091500000"; // 09:15:00.000
            String endTime = datePart + "092500000"; // 09:25:00.000
            return "skip";
        }
        // 判断是否在上午连续竞价时间段 (09:30 - 11:30)
        else if (timeStr.compareTo("0930") >= 0 && timeStr.compareTo("1130") <= 0) {
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            int min = Integer.parseInt(timeStr.substring(2, 4));
            int totalMinutes = (hour - 9) * 60 + min - 30;
            int segment = totalMinutes / k;

            int return_min =30+ segment * k;
            int return_hour = 9 + return_min / 60;
            String returnTimeStart = String.format("%02d:%02d", return_hour, return_min % 60);

            // 计算结束时间
            return_min = 30+ (segment + 1) * k;
            return_hour = 9 + return_min / 60;
            String returnTimeEnd = String.format("%02d:%02d", return_hour, return_min % 60);

            String startTime = datePart + returnTimeStart.replace(":", "") + "00000"; // start time
            String endTime = datePart + returnTimeEnd.replace(":", "") + "00000"; // end time
            return startTime + "to" + endTime;
        }
        // 判断是否在下午连续竞价时间段 (13:00 - 14:57) // 特别修改为15:00
        else if (timeStr.compareTo("1300") >= 0 && timeStr.compareTo("1500") <= 0) {
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            int min = Integer.parseInt(timeStr.substring(2, 4));
            int totalMinutes = (hour - 13) * 60 + min;
            int segment = totalMinutes / k;

            if (timeStr.compareTo("1500") == 0) {
                segment -= 1;
            }// 特别修改为15:00

            int return_min = segment * k;
            int return_hour = 13 + return_min / 60;
            String returnTimeStart = String.format("%02d:%02d", return_hour, return_min % 60);

            // 计算结束时间
            return_min = (segment + 1) * k;
            return_hour = 13 + return_min / 60;
            String returnTimeEnd = String.format("%02d:%02d", return_hour, return_min % 60);

            String startTime = datePart + returnTimeStart.replace(":", "") + "00000"; // start time
            String endTime = datePart + returnTimeEnd.replace(":", "") + "00000"; // end time
            return startTime + "to" + endTime;
        }
       /*  // 判断是否在收盘集合竞价时间段 (14:57 - 15:00)
        else if (timeStr.compareTo("1457") >= 0 && timeStr.compareTo("1500") <= 0) {
            String startTime = datePart + "1457000000"; // 14:57:00.000
            String endTime = datePart + "1500000000"; // 15:00:00.000
            return startTime + "to" + endTime;
        }*/
        return timeStr; // 如果时间戳不在任何预定义的时间段内

    }
}