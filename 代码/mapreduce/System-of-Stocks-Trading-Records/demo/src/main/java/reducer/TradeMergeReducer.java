package reducer;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

// TradeMergeReducer类继承自Hadoop的Reducer抽象类，其主要作用是对Mapper阶段输出的中间键值对（这里键为Text类型，值为Text类型）进行归约处理，汇总计算相同键对应的交易数据中的交易数量和交易金额总和，并输出最终的结果。
public class TradeMergeReducer extends Reducer<Text, Text, Text, Text> {

    // 用于存储最终要输出的结果信息，类型为Text，后续会根据业务逻辑设置其具体内容后输出。
    private Text result = new Text();

    // 重写父类Reducer的reduce方法，该方法会针对每个不同的键（在Mapper阶段输出的键）及其对应的一组值（具有相同键的所有Mapper输出的值的集合）进行调用，执行具体的归约逻辑。
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 用于累计交易金额总和，初始化为0.0，在遍历过程中会将每个对应值中的交易金额累加到该变量中。
        Double totalTradeAmount = 0.0;
        // 用于累计交易数量总和，初始化为0L，通过解析每个对应值中的交易数量并进行累加来计算总的交易数量。
        Long totalTradeQty = 0L;

        // 遍历与当前键对应的所有Text类型的值（这些值是在Mapper阶段输出的，具有相同的键），进行数据的累计统计。
        for (Text value : values) {
            // 将Text类型的值转换为String类型，方便后续按逗号分隔符进行拆分获取交易数量和交易金额信息。
            String[] parts = value.toString().split(",");
            // 将拆分出的交易金额字段从String类型转换为Double类型，并累加到总交易金额变量中。
            totalTradeAmount += Double.parseDouble(parts[1]);
            // 将拆分出的交易数量字段从String类型转换为Long类型，并累加到总交易数量变量中。
            totalTradeQty += Long.parseLong(parts[0]);
        }

        // 设置要输出的结果文本内容，格式为<交易数量,交易金额>，将累计计算得到的总交易数量和总交易金额以逗号分隔的形式进行设置。
        result.set(totalTradeQty + "," + totalTradeAmount);

        // 将键值对（键是Mapper阶段传过来的原始键，值是经过汇总计算后的结果文本）写入到Hadoop的上下文对象中，这样数据就可以按照MapReduce框架的流程进行后续处理或者输出到指定位置了。
        context.write(key, result);
    }
}