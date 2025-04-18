package reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// ConverReducer类继承自Hadoop的Reducer抽象类，用于对Mapper输出的中间键值对进行归约处理，汇总计算并输出最终的结果
public class ConverReducer extends Reducer<Text, Text, Text, Text> {

    // 添加标志位，用于确保表头只输出一次。在整个Reduce任务处理过程中，第一次执行reduce方法时输出表头信息，后续则不再输出
    private boolean firstOutput = true;

    // 重写父类Reducer的reduce方法，该方法会针对每个不同的键（在Mapper阶段输出的键）及其对应的一组值（具有相同键的所有Mapper输出的值的集合）进行调用，执行具体的归约逻辑
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 如果是第一次执行reduce方法（即处理第一组数据时），输出表头信息
        if (firstOutput) {
            // 构建表头字符串，包含了最终输出结果中各列的名称，以逗号分隔，对应不同的业务指标
            String header = "主力净流入,主力流入,主力流出,超大买单成交量,超大买单成交额,超大卖单成交量,超大卖单成交额,大买单成交量,大买单成交额,大卖单成交量,大卖单成交额,中买单成交量,中买单成交额,中卖单成交量,中卖单成交额,小买单成交量,小买单成交额,小卖单成交量,小卖单成交额,时间区间";
            // 输出表头，这里键设为null（具体含义可能根据输出格式要求而定，也许在后续写入文件等操作时有特殊处理方式），值为包含表头信息的Text类型对象
            context.write(null, new Text(header));
            // 将标志位修改为false，避免后续再次输出表头
            firstOutput = false;
        }

        // 声明各种买卖单（超大、大、中、小）的成交量变量，并初始化为0，用于累计对应类别的成交量数据
        long buy_extraLargeVolume = 0, buy_largeVolume = 0, buy_mediumVolume = 0, buy_smallVolume = 0;
        // 声明各种买卖单（超大、大、中、小）的成交额变量，并初始化为0.0，用于累计对应类别的成交额数据
        double buy_extraLargeAmount = 0, buy_largeAmount = 0, buy_mediumAmount = 0, buy_smallAmount = 0;

        long sell_extraLargeVolume = 0, sell_largeVolume = 0, sell_mediumVolume = 0, sell_smallVolume = 0;
        double sell_extraLargeAmount = 0, sell_largeAmount = 0, sell_mediumAmount = 0, sell_smallAmount = 0;

        // 遍历与当前键对应的所有值（这些值是在Mapper阶段输出的，具有相同的键），进行数据的累计统计
        for (Text value : values) {
            // 将Text类型的值转换为String类型，方便后续按特定分隔符进行拆分获取各个字段信息
            String[] parts = value.toString().split("_");
            // 获取是否是买单的标识信息（"Buy"或"Sell"），用于后续判断是买入还是卖出操作
            String isBuy = parts[0];
            // 获取交易类别信息（如"Large"、"Medium"、"Small"、"ExtraLarge"），用于确定属于哪种规模的交易
            String category = parts[1];
            // 将成交量字段从String类型转换为long类型，以便进行数值的累计计算
            long volume = Long.parseLong(parts[2]);
            // 将成交额字段从String类型转换为double类型，以便进行数值的累计计算
            double amount = Double.parseDouble(parts[3]);

            // 如果是买单
            if ("Buy".equals(isBuy)) {
                // 根据不同的交易类别，将对应的成交量和成交额累加到相应的变量中
                switch (category) {
                    case "ExtraLarge":
                        buy_extraLargeVolume = volume;
                        buy_extraLargeAmount = amount;
                        break;
                    case "Large":
                        buy_largeVolume = volume;
                        buy_largeAmount = amount;
                        break;
                    case "Medium":
                        buy_mediumVolume = volume;
                        buy_mediumAmount = amount;
                        break;
                    case "Small":
                        buy_smallVolume = volume;
                        buy_smallAmount = amount;
                        break;
                    default:
                        break;
                }
            } else if ("Sell".equals(isBuy)) { // 如果是卖单
                // 根据不同的交易类别，将对应的成交量和成交额累加到相应的变量中
                switch (category) {
                    case "ExtraLarge":
                        sell_extraLargeVolume = volume;
                        sell_extraLargeAmount = amount;
                        break;
                    case "Large":
                        sell_largeVolume = volume;
                        sell_largeAmount = amount;
                        break;
                    case "Medium":
                        sell_mediumVolume = volume;
                        sell_mediumAmount = amount;
                        break;
                    case "Small":
                        sell_smallVolume = volume;
                        sell_smallAmount = amount;
                        break;
                    default:
                        break;
                }
            }
        }

        // 计算主力净流入，即主力流入减去主力流出，主力流入为大买单和超大买单的成交额之和
        double inflow = buy_largeAmount + buy_extraLargeAmount;
        // 计算主力流出，为大卖单和超大卖单的成交额之和
        double outflow = sell_largeAmount + sell_extraLargeAmount;
        double netFlow = inflow - outflow;

        // 获取时间范围信息（键对应的内容），并对其格式进行适当调整（例如将"to"替换为" to "，使其格式更符合展示要求）
        String timeRange = key.toString();
        timeRange = timeRange.replace("to", " to ");

        // 使用String.format方法按照指定的格式构建最终要输出的结果字符串，包含了计算好的各项业务指标以及时间范围信息，各指标以逗号分隔，格式化为保留两位小数等要求
        String result = String.format(
                "%.2f,%.2f,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%s",
                netFlow, inflow, outflow,
                buy_extraLargeVolume, buy_extraLargeAmount,
                sell_extraLargeVolume, sell_extraLargeAmount,
                buy_largeVolume, buy_largeAmount,
                sell_largeVolume, sell_largeAmount,
                buy_mediumVolume, buy_mediumAmount,
                sell_mediumVolume, sell_mediumAmount,
                buy_smallVolume, buy_smallAmount,
                sell_smallVolume, sell_smallAmount,
                timeRange);

        // 将最终的结果输出，键设为null（同样具体含义与输出格式相关），值为包含最终结果信息的Text类型对象
        context.write(null, new Text(result));
    }
}