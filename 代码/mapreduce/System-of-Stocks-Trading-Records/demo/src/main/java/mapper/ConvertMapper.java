package mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// ConvertMapper类继承自Hadoop的Mapper抽象类，用于对输入数据进行特定的映射处理，将输入数据转换为适合后续Reducer处理的中间键值对格式
public class ConvertMapper extends Mapper<Object, Text, Text, Text> {
    // 重写父类Mapper的map方法，该方法会针对输入数据的每一行被调用，用于执行具体的映射逻辑
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将Hadoop的Text类型的输入值转换为Java的String类型，方便后续的字符串操作，这里的value代表输入的一行数据
        String line = value.toString();
        // 以制表符（\t）为分隔符，将输入的一行数据拆分成字符串数组，每个元素对应不同的字段信息
        String[] fields = line.split("\t");

        // 如果拆分后的字段数量不等于2，说明该行数据格式不符合预期，直接跳过这一行，不进行后续处理
        if (fields.length != 2) {
            return;
        }

        // 对第一个字段（通常包含了多个部分，以某种分隔符分开的复合信息）再以"_"进行拆分，获取其中的关键部分信息
        String[] keyParts = fields[0].split("_");
        // 如果拆分后的部分数量小于4，可能意味着时间格式等关键信息不完整或不正确，跳过这一行
        if (keyParts.length < 4) {
            return;
        }

        // 获取时间信息，例如类似"09:25"这样的格式，从拆分后的第一个字段的相关部分中提取，用于后续构建输出的键
        String time = keyParts[2];
        // 获取类别信息，如"Large"、"Medium"、"Small"、"ExtraLarge"等，同样从第一个字段的对应部分提取，用于构建输出的值
        String category = keyParts[3];
        // 获取是买入（Buy）还是卖出（Sell）的标识信息，从第一个字段的相关部分获取，用于构建输出的值
        String isBuy = keyParts[0];

        // 对第二个字段（通常也是包含多个信息的复合字段）以逗号（,）为分隔符进行拆分，获取成交量和成交额等具体数据信息
        String[] values = fields[1].split(",");
        // 如果拆分后的元素数量不等于2，说明成交量和成交额的格式不符合要求，跳过这一行
        if (values.length != 2) {
            return;
        }

        // 获取成交量信息，用于构建输出的值
        String volume = values[0];
        // 获取成交额信息，用于构建输出的值
        String amount = values[1];

        // 使用Hadoop的Text类型构建输出的键，这里将时间作为键，意味着后续相同时间的数据会被聚合到一起处理（在Reducer阶段）
        // 构建输出的值，格式为<买入或卖出标识_类别_成交量_成交额>，包含了这一行数据经过提取和整理后的关键业务信息
        context.write(new Text(time), new Text(isBuy + "_" + category + "_" + volume + "_" + amount));
    }
}