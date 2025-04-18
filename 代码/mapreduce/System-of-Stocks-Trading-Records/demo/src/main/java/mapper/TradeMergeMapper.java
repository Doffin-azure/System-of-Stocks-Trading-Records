package mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

// TradeMergeMapper类继承自Hadoop的Mapper抽象类，用于对输入数据进行映射处理，按照特定的业务规则对交易数据进一步分类、整合，并转换为适合后续Reducer处理的中间键值对格式。
public class TradeMergeMapper extends Mapper<Object, Text, Text, Text> {
    // 股票总量，初始值设为17170245800L，后续会从配置中获取实际值进行更新，用于判断交易单的规模等业务逻辑。
    Long allStock = 17170245800L;
    // 用于计算时间窗口的参数，初始值设为20，同样可从配置中获取实际值，用于划分交易时间区间等操作。
    int k = 20;
    // 用于存储输出键的对象，类型为Text，后续会根据业务逻辑设置其具体内容并输出。
    private Text keyOut = new Text();
    // 用于存储输出值的对象，类型为Text，按业务逻辑赋值后输出相应的数据信息。
    private Text valueOut = new Text();

    // setup方法会在Mapper任务启动时被调用一次，用于进行一些初始化的配置操作，从配置中获取相关参数值。
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 从Hadoop的任务配置中获取名为"allStock"的长整型配置参数，如果不存在则使用默认值17170245800L，以此来设置股票总量的值。
        allStock = context.getConfiguration().getLong("allStock", 17170245800L);
        // 从配置中获取名为"timeWindow"的整型配置参数，若不存在则使用默认值10，用于设置时间窗口相关的参数k的值。
        k = context.getConfiguration().getInt("timeWindow", 10);
        System.out.println(k);
    }

    // 重写父类Mapper的map方法，该方法会针对输入数据的每一行被调用，执行具体的映射逻辑，对交易数据进行分类、构建新的键值对。
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将Hadoop的Text类型的输入值转换为Java的String类型，方便后续的字符串操作，这里的value代表输入的一行数据。
        String line = value.toString();
        // 以制表符（\t）为分隔符，将输入的一行数据拆分成字符串数组，每个元素对应不同的字段信息。
        String[] parts = line.split("\t");
        // 将拆分后的第一个元素（可能原本是键相关信息）重新构建为Text类型，此处代码逻辑可能有点冗余，因为key本身就是Object类型可接收多种情况，不过可能是为了明确类型转换。
        key = new Text(parts[0]);
        // 将拆分后的第二个元素（可能原本是值相关信息）重新构建为Text类型。
        value = new Text(parts[1]);

        // 对重新构建后的键（Text类型）再以"_"进行拆分，获取其中不同部分的信息，用于后续业务逻辑判断。
        String[] columns = key.toString().split("_");
        // 获取时间戳信息，用于后续判断所属交易时间区间等操作。
        String timeStamp = columns[3];
        // 获取订单类型信息（如买入或卖出等），用于构建输出键等业务逻辑。
        String orderType = columns[0];
        // 获取安全标识信息（格式为长整型对应的字符串表示），用于构建输出键等操作。
        String securityId = columns[1];

        // 对值（Text类型）以逗号（,）为分隔符进行拆分，获取交易数量和交易金额等信息。
        String[] values = value.toString().split(",");

        // 将交易数量字段从String类型转换为Long类型，以便后续进行数值比较等业务逻辑操作。
        Long tradeQty = Long.parseLong(values[0]);
        // 将交易金额字段从String类型转换为Double类型，用于后续判断交易单规模等操作。
        Double tradeAmount = Double.parseDouble(values[1]);

        // 用于存储交易单类型的字符串，初始为空，后续会根据交易数量、交易金额等条件判断来确定具体类型（超大单、大单、中单、小单）。
        String tradeType = "";

        // 判断是否为超大单，根据交易数量、交易金额以及交易数量与股票总量的占比等条件来确定，如果满足以下任一条件则判定为超大单。
        if (tradeQty >= 200000 || tradeAmount >= 1000000 || (double) tradeQty / allStock >= 0.003) {
            tradeType = "ExtraLarge";
        }
        // 判断是否为大单，若不满足超大单条件但满足以下条件则判定为大单。
        else if (tradeQty >= 60000 || tradeAmount >= 300000 || (double) tradeQty / allStock >= 0.001) {
            tradeType = "Large";
        }
        // 判断是否为中单，若不满足大单条件但满足以下条件则判定为中单。
        else if (tradeQty >= 10000 || tradeAmount >= 50000 || (double) tradeQty / allStock >= 0.00017) {
            tradeType = "Medium";
        } else {
            // 如果都不满足上述条件，则判定为小单。
            tradeType = "Small";
        }

        // 用于存储交易时间区间的字符串，后续会根据时间戳以及相关业务规则来确定具体的时间区间范围。
        String tradingTimeSegment;
        try {
            // 调用自定义的方法根据时间戳获取对应的交易时间区间，如果返回"skip"表示该时间戳对应的时间段需要跳过（比如集合竞价等特殊时段），则直接返回不进行后续输出操作。
            tradingTimeSegment = getTradingTimeSegment(timeStamp);
            if (tradingTimeSegment.equals("skip")) {
                return;
            }
            // 构建输出键，格式为<订单类型_安全标识_交易时间区间_交易单类型>，整合了相关业务信息，方便后续Reducer按这些维度进行聚合处理。
            keyOut.set(orderType + "_" + securityId + "_" + tradingTimeSegment + "_" + tradeType);
            // 构建输出值，格式为<交易数量,交易金额>，包含了这笔交易的关键数据信息。
            valueOut.set(tradeQty + "," + tradeAmount);
            // 将构建好的键值对写入到Hadoop的上下文对象中，以便传递到Reducer阶段进行后续处理。
            context.write(keyOut, valueOut);
        } catch (ParseException e) {
            // 如果在解析时间戳等操作出现异常，打印异常堆栈信息，方便排查问题。
            e.printStackTrace();
        }
    }

    // 自定义方法，根据给定的时间戳字符串，按照特定的业务规则判断其所属的交易时间区间，并返回对应的时间区间表示字符串。
    String getTradingTimeSegment(String timeStamp) throws ParseException {
        // 创建一个SimpleDateFormat对象，用于将时间戳字符串解析为Date类型，格式按照"yyyyMMddHHmmssSSS"进行解析。
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        Date date = sdf.parse(timeStamp);

        // 创建一个新的SimpleDateFormat对象，用于将解析后的Date类型时间按照"HHmm"格式进行格式化输出，获取时间部分的字符串表示（如"0930"表示9点30分）。
        SimpleDateFormat outputFormat = new SimpleDateFormat("HHmm");
        String timeStr = outputFormat.format(date);

        // 创建另一个SimpleDateFormat对象，用于获取时间戳对应的日期部分，格式为"yyyyMMdd"，后续用于构建完整的时间区间字符串。
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String datePart = dateFormat.format(date);

        // 判断时间是否在开盘集合竞价时间段 (09:15 - 09:25)，如果在该时间段内，则返回"skip"表示需要跳过该时间段的数据处理。
        if (timeStr.compareTo("0915") >= 0 && timeStr.compareTo("0925") <= 0) {
            String startTime = datePart + "091500000"; // 09:15:00.000
            String endTime = datePart + "092500000"; // 09:25:00.000
            return "skip";
        }
        // 判断时间是否在上午连续竞价时间段 (09:30 -
        // 11:30)，如果在该时间段内，则按照时间窗口参数k进行时间区间划分，并返回对应的时间区间范围字符串（格式为"开始时间to结束时间"）。
        else if (timeStr.compareTo("0930") >= 0 && timeStr.compareTo("1130") <= 0) {
            // 从时间字符串中解析出小时部分并转换为整数。
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            // 从时间字符串中解析出分钟部分并转换为整数。
            int min = Integer.parseInt(timeStr.substring(2, 4));
            // 计算从9点30分开始到当前时间经过的分钟数，用于后续按时间窗口划分区间。
            int totalMinutes = (hour - 9) * 60 + min - 30;
            // 根据总分钟数和时间窗口参数k计算所属的时间区间段序号。
            int segment = totalMinutes / k;

            // 计算该时间区间段的起始分钟数，基于时间窗口参数和起始的9点30分进行换算。
            int return_min = 30 + segment * k;
            // 计算该时间区间段的起始小时数，根据起始分钟数换算得到。
            int return_hour = 9 + return_min / 60;
            // 格式化输出起始时间字符串，格式为"HH:mm"。
            String returnTimeStart = String.format("%02d:%02d", return_hour, return_min % 60);

            // 计算该时间区间段的结束分钟数，用于构建完整的时间区间范围。
            return_min = 30 + (segment + 1) * k;
            // 计算该时间区间段的结束小时数。
            return_hour = 9 + return_min / 60;
            // 格式化输出结束时间字符串，格式为"HH:mm"。
            String returnTimeEnd = String.format("%02d:%02d", return_hour, return_min % 60);

            // 构建完整的起始时间字符串，包含日期部分以及格式化后的时间部分，格式为"yyyyMMddHHmmssSSS"。
            String startTime = datePart + returnTimeStart.replace(":", "") + "00000";
            // 构建完整的结束时间字符串，同样包含日期部分以及格式化后的时间部分。
            String endTime = datePart + returnTimeEnd.replace(":", "") + "00000";
            return startTime + "to" + endTime;
        }
        // 判断时间是否在下午连续竞价时间段 (13:00 -
        // 15:00)，如果在该时间段内，同样按照时间窗口参数k进行时间区间划分，并返回对应的时间区间范围字符串。
        else if (timeStr.compareTo("1300") >= 0 && timeStr.compareTo("1500") <= 0) {
            // 从时间字符串中解析出小时部分并转换为整数。
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            // 从时间字符串中解析出分钟部分并转换为整数。
            int min = Integer.parseInt(timeStr.substring(2, 4));
            // 计算从13点开始到当前时间经过的分钟数，用于后续按时间窗口划分区间。
            int totalMinutes = (hour - 13) * 60 + min;
            // 根据总分钟数和时间窗口参数k计算所属的时间区间段序号。
            int segment = totalMinutes / k;

            // 特别修改，当时间为15:00时，将区间段序号减1，可能是基于业务上对该时间点所属区间的特殊定义。
            if (timeStr.compareTo("1500") == 0) {
                segment -= 1;
            }

            // 计算该时间区间段的起始分钟数，基于时间窗口参数和起始的13点进行换算。
            int return_min = segment * k;
            // 计算该时间区间段的起始小时数，根据起始分钟数换算得到。
            int return_hour = 13 + return_min / 60;
            // 格式化输出起始时间字符串，格式为"HH:mm"。
            String returnTimeStart = String.format("%02d:%02d", return_hour, return_min % 60);

            // 计算该时间区间段的结束分钟数，用于构建完整的时间区间范围。
            return_min = (segment + 1) * k;
            // 计算该时间区间段的结束小时数。
            return_hour = 13 + return_min / 60;
            // 格式化输出结束时间字符串，格式为"HH:mm"。
            String returnTimeEnd = String.format("%02d:%02d", return_hour, return_min % 60);

            // 构建完整的起始时间字符串，包含日期部分以及格式化后的时间部分，格式为"yyyyMMddHHmmssSSS"。
            String startTime = datePart + returnTimeStart.replace(":", "") + "00000";
            // 构建完整的结束时间字符串，同样包含日期部分以及格式化后的时间部分。
            String endTime = datePart + returnTimeEnd.replace(":", "") + "00000";
            return startTime + "to" + endTime;
        }
        // 原本此处有判断是否在收盘集合竞价时间段 (14:57 - 15:00)的逻辑，但被注释掉了，可能根据业务需求暂时不需要该部分判断或者后续会有其他处理方式。
        // 若时间戳不在上述任何预定义的时间段内，则直接返回时间部分的字符串表示（如"HHmm"格式）。
        return timeStr;
    }
}