package reducer;

import util.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

// TradeFilterReducer类继承自Hadoop的Reducer抽象类，其主要作用是对Mapper阶段输出的中间键值对（这里键为Text类型，值为TradeData类型）进行归约处理，汇总计算并输出最终的结果。
public class TradeFilterReducer extends Reducer<Text, TradeData, Text, Text> {
    // 用于存储最终要输出的结果信息，类型为Text，后续会根据业务逻辑设置其具体内容后输出。
    private Text result = new Text();

    // 重写父类Reducer的reduce方法，该方法会针对每个不同的键（在Mapper阶段输出的键）及其对应的一组值（具有相同键的所有Mapper输出的值的集合）进行调用，执行具体的归约逻辑。
    @Override
    protected void reduce(Text key, Iterable<TradeData> values, Context context)
            throws IOException, InterruptedException {
        // 用于累计交易数量，初始化为0，在遍历过程中会将每个TradeData中的交易数量累加到该变量中。
        int totalTradeQty = 0;
        // 用于累计交易成交额，初始化为0.0，通过每个TradeData中的价格与交易数量相乘后进行累加来计算总的成交额。
        double totalTradeAmount = 0;

        // 遍历与当前键对应的所有TradeData值（这些值是在Mapper阶段输出的，具有相同的键），进行数据的累计统计。
        for (TradeData data : values) {
            // 将每个TradeData中的交易数量累加到总交易数量变量中。
            totalTradeQty += data.getTradeQty();
            // 根据成交额 = 价格 × 交易数量的公式，计算每个TradeData对应的成交额，并累加到总成交额变量中。
            totalTradeAmount += data.getPrice() * data.getTradeQty();
        }

        // 设置要输出的结果文本内容，格式为<交易数量,成交额>，将累计计算得到的总交易数量和总成交额以逗号分隔的形式进行设置。
        result.set(totalTradeQty + "," + totalTradeAmount);

        // 将键值对（键是Mapper阶段传过来的原始键，值是经过汇总计算后的结果文本）写入到Hadoop的上下文对象中，这样数据就可以按照MapReduce框架的流程进行后续处理或者输出到指定位置了。
        context.write(key, result);
    }
}