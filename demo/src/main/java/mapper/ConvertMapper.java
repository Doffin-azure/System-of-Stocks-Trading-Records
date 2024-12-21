package mapper;



import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConvertMapper extends Mapper<Object, Text, Text, Text> {
    // 解析每一行输入数据
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        if (fields.length != 2) {
            return; // 跳过格式不对的行
        }

        String[] keyParts = fields[0].split("_");
        if (keyParts.length < 4) {
            return; // 如果时间格式不正确，跳过
        }

        String time = keyParts[2]; // 获取时间（例如：09:25）
        String category = keyParts[3]; // 获取类别（Large, Medium, Small, ExtraLarge）
        String isBuy = keyParts[0];// Buy or Sell

        String[] values = fields[1].split(",");
        if (values.length != 2) {
            return; // 如果成交量和成交额格式不对，跳过
        }

        String volume = values[0];
        String amount = values[1];

        // 输出数据格式为 <时间, 类别_成交量_成交额>
        context.write(new Text(time), new Text(isBuy+"_"+category + "_" + volume + "_" + amount));
    }
}