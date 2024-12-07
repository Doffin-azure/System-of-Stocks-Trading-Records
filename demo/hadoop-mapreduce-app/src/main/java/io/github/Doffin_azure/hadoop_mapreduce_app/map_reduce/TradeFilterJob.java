import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TradeFilterJob {
    public static void main(String[] args) throws Exception {
        // 确保命令行参数正确
        if (args.length != 2) {
            System.err.println("Usage: TradeFilterJob <input path> <output path>");
            System.exit(-1);
        }

        // 创建Hadoop作业配置
        Configuration conf = new Configuration();
        
        // 创建一个新的MapReduce作业
        Job job = Job.getInstance(conf, "Trade Filter Job");

        // 设置作业的Jar文件
        job.setJarByClass(TradeFilterJob.class);
        
        // 设置Mapper和Reducer类
        job.setMapperClass(TradeFilterMapper.class);
        job.setReducerClass(TradeFilterReducer.class);

        // 设置Map的输出类型（<Text, TradeStats>）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TradeStats.class);

        // 设置输入格式和输出格式
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
