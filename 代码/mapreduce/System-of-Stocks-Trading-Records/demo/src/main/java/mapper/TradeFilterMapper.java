package mapper;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import util.*;

// TradeFilterMapper类继承自Hadoop的Mapper抽象类，用于对输入数据进行映射处理，筛选并转换符合特定条件的数据为中间键值对格式，供后续Reducer进一步处理
public class TradeFilterMapper extends Mapper<Object, Text, Text, TradeData> {
    // 用于存储输出键的对象，类型为Text，后续会根据业务逻辑设置其具体内容并输出
    private Text keyOut = new Text();
    // 用于存储输出值的对象，类型为自定义的TradeData，包含了交易相关的数据信息，同样会按业务逻辑进行赋值后输出
    private TradeData valueOut = new TradeData();
    // 目标安全标识，初始值设为1，后续会从配置中获取实际值进行更新，用于筛选特定安全标识的数据
    private int aimSecurityID = 1;

    // setup方法会在Mapper任务启动时被调用一次，用于进行一些初始化的配置操作
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 从Hadoop的任务配置中获取名为"securityID"的整数配置参数，如果不存在则使用默认值1，以此来设置目标安全标识的值
        aimSecurityID = context.getConfiguration().getInt("securityID", 1);
    }

    // 重写父类Mapper的map方法，该方法会针对输入数据的每一行被调用，执行具体的映射逻辑，筛选并转换符合条件的数据
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将Hadoop的Text类型的输入值转换为Java的String类型，方便后续的字符串操作，这里的value代表输入的一行数据
        String tradeLine = value.toString();
        // 以制表符（\t）为分隔符，将输入的一行数据拆分成字符串数组，每个元素对应不同的字段信息
        String[] columns = tradeLine.split("\t");

        // 从拆分后的字段数组中解析出安全标识字段，并转换为整数类型，用于后续的条件判断
        int securityID = Integer.parseInt(columns[8]);
        // 解析出出价应用序列号字段，并转换为长整型，用于判断买单卖单等业务逻辑
        long bidAppSeqNum = Long.parseLong(columns[10]);
        // 解析出要价应用序列号字段，并转换为长整型，同样用于业务逻辑判断
        long offerAppSeqNum = Long.parseLong(columns[11]);
        // 解析出价格字段，并转换为双精度浮点型，后续用于计算成交额等操作
        double price = Double.parseDouble(columns[12]);
        // 解析出交易数量字段，并转换为整数类型，作为交易数据的一部分
        int tradeQty = Integer.parseInt(columns[13]);
        // 解析出执行类型字段，用于判断是否符合特定的业务执行类型条件
        String execType = columns[14];
        // 解析出时间戳字段，并转换为长整型，可能用于构建输出键等业务逻辑
        Long timeStamp = Long.parseLong(columns[15]);

        // 如果执行类型为"F"（这里应该是基于业务定义的一种执行类型，表示符合特定业务规则的交易情况）
        if ("F".equals(execType)) {
            // 判断当前行数据的安全标识是否与目标安全标识相等，只有相等的才进行后续处理，起到筛选特定安全标识数据的作用
            if (securityID == aimSecurityID) {
                // 判断是买单还是卖单，根据出价应用序列号和要价应用序列号的大小关系来确定
                String orderType;
                if (bidAppSeqNum > offerAppSeqNum) {
                    orderType = "Buy"; // 买单
                    // 构建输出键，格式为<订单类型_安全标识_应用序列号_时间戳>，将相关业务信息整合到键中，方便后续聚合等处理
                    keyOut.set(orderType + "_" + securityID + "_" + bidAppSeqNum + "_" + timeStamp);
                } else {
                    orderType = "Sell"; // 卖单
                    // 同样构建输出键，只是应用序列号部分根据卖单的逻辑选取相应的值
                    keyOut.set(orderType + "_" + securityID + "_" + offerAppSeqNum + "_" + timeStamp);
                }

                // 创建一个TradeData对象，用于存储交易数量和价格信息，以此来表示这笔交易的相关数据情况，作为输出的值
                valueOut = new TradeData(tradeQty, price);

                // 将构建好的键值对写入到Hadoop的上下文对象中，这样数据就可以传递到Reducer阶段进行后续处理
                context.write(keyOut, valueOut);
            }
        }
    }
}