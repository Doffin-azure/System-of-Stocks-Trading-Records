package reducer ;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ConverReducer extends Reducer<Text, Text, Text, Text> {
    // 声明各种买卖单的成交量和成交额变量
    long buy_extraLargeVolume = 0, buy_largeVolume = 0, buy_mediumVolume = 0, buy_smallVolume = 0;
    double buy_extraLargeAmount = 0, buy_largeAmount = 0, buy_mediumAmount = 0, buy_smallAmount = 0;

    long sell_extraLargeVolume = 0, sell_largeVolume = 0, sell_mediumVolume = 0, sell_smallVolume = 0;
    double sell_extraLargeAmount = 0, sell_largeAmount = 0, sell_mediumAmount = 0, sell_smallAmount = 0;


    // 添加标志位，确保表头只输出一次
    private boolean firstOutput = true;
    // 计算每个时间段的指标
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (firstOutput) {
            String header = "主力净流入, 主力流入, 主力流出, 超大买单成交量, 超大买单成交额, 超大卖单成交量, 超大卖单成交额, 大买单成交量, 大买单成交额, 大卖单成交量, 大卖单成交额, 中买单成交量, 中买单成交额, 中卖单成交量, 中卖单成交额, 小买单成交量, 小买单成交额, 小卖单成交量, 小卖单成交额, 时间范围";
            context.write(new Text("header"), new Text(header));  // 输出表头
            firstOutput = false;  // 修改标志位，防止后续再输出表头
        }

        // 累计每个类别的成交量和成交额
        for (Text value : values) {
            String[] parts = value.toString().split("_");
            String isBuy = parts[0];  // 是否是买单
            String category = parts[1];  // 类别（Large, Medium, Small, ExtraLarge）
            long volume = Long.parseLong(parts[2]);  // 成交量
            double amount = Double.parseDouble(parts[3]);  // 成交额

            if ("Buy".equals(isBuy)) {  // 如果是买单
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
            } else if ("Sell".equals(isBuy)) {  // 如果是卖单
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

        // 计算主力净流入、主力流入和主力流出
        double inflow = buy_largeVolume + buy_extraLargeAmount; // 主力流入
        double outflow = sell_largeAmount + sell_extraLargeVolume; // 主力流出
        double netFlow = inflow - outflow;
        String timeRange = key.toString();
        // 输出结果
        String result = String.format(
            "%.2f, %.2f, %.2f, %d, %.2f, %d, %.2f, %d, %.2f, %d, %.2f, %d, %.2f, %d, %.2f, %d, %.2f, %d, %.2f, %d, %.2f, %s",
            netFlow, inflow, outflow,
            buy_extraLargeVolume, buy_extraLargeAmount,
            sell_extraLargeVolume, sell_extraLargeAmount,
            buy_largeVolume, buy_largeAmount,
            sell_largeVolume, sell_largeAmount,
            buy_mediumVolume, buy_mediumAmount,
            sell_mediumVolume, sell_mediumAmount,
            buy_smallVolume, buy_smallAmount,
            sell_smallVolume, sell_smallAmount,
            timeRange
        );

        context.write(null, new Text(result));
    }
}