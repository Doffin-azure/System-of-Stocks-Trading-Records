package driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import mapper.*;
import reducer.*;
import util.TradeData;

// 主类，用于控制一系列MapReduce任务的执行流程，包括设置任务参数、启动任务等
public class TradeController {

    public static void main(String[] args) throws Exception {
        // 初始化安全标识，默认值为1，后续可从命令行参数获取实际值进行替换
        int securityID = 1;
        // 初始化时间窗口，默认值为10，后续可从命令行参数获取实际值进行替换
        int timeWindow = 10;
        // 初始化股票总量，这里只是示例赋值，具体含义需结合业务场景确定
        long allStock = 17170245800L;

        try {
            // 尝试从命令行参数中解析出安全标识，如果参数格式不正确会进入catch块
            securityID = Integer.parseInt(args[4]);
            // 尝试从命令行参数中解析出时间窗口，如果参数格式不正确会进入catch块
            timeWindow = Integer.parseInt(args[5]);
        } catch (Exception e) {
            // 如果解析参数出现异常，打印提示信息，告知用户需要输入安全标识和时间窗口参数
            System.out.println("Please input the securityID and timeWindow");
        }

        // Step 1: 设置并执行第一个MapReduce任务（TradeFilter任务）
        int exitCode = runFirstMapReduceJob(args[0], args[1], securityID);
        if (exitCode != 0) {
            // 如果第一个MapReduce任务执行失败，打印错误信息并以相应的退出码退出程序
            System.err.println("First MapReduce job failed!");
            System.exit(exitCode);
        }

        // Step 2: 设置并执行第二个MapReduce任务（TradeMerge任务）
        exitCode = runSecondMapReduceJob(args[1], args[2], timeWindow);
        if (exitCode != 0) {
            // 如果第二个MapReduce任务执行失败，打印错误信息并以相应的退出码退出程序
            System.err.println("Second MapReduce job failed!");
            System.exit(exitCode);
        }

        // Step 3: 添加并执行第三个MapReduce任务（Conver任务）
        exitCode = runThirdMapReduceJob(args[2], args[3]); // 添加第三个作业
        if (exitCode != 0) {
            // 如果第三个MapReduce任务执行失败，打印错误信息并以相应的退出码退出程序
            System.err.println("Third MapReduce job failed!");
            System.exit(exitCode);
        }

        // 程序正常结束，退出码设为0
        System.exit(0);
    }

    // 用于设置并运行第一个MapReduce任务（TradeFilter任务）的私有方法
    private static int runFirstMapReduceJob(String inputPath, String outputPath, int securityID)
            throws IOException, InterruptedException, ClassNotFoundException {
        // 创建一个新的Hadoop配置对象，用于设置任务相关的各种配置参数
        Configuration conf = new Configuration();
        // 在配置中设置安全标识参数，这个参数会传递到MapReduce任务中供相关逻辑使用
        conf.setInt("securityID", securityID);

        // 根据配置创建一个MapReduce任务实例，任务名称为"TradeFilter"
        Job job = Job.getInstance(conf, "TradeFilter");

        // 设置任务的主类，Hadoop框架会根据这个类来查找相关的资源（如Mapper、Reducer等类）
        job.setJarByClass(TradeController.class);

        // 设置输入数据的路径，将指定的输入路径添加到任务的输入路径列表中，这里使用的是文本格式的输入
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // 设置任务的输入数据格式为文本格式，意味着输入的数据将按文本行进行读取和处理
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出数据的路径，指定任务处理后结果的输出位置，这里使用的是文本格式输出
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 设置任务的输出数据格式为文本格式，意味着输出的数据将按文本格式进行保存
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置任务使用的Mapper类，这个类负责对输入数据进行映射处理，生成中间键值对
        job.setMapperClass(TradeFilterMapper.class);
        // 设置Mapper输出的键的数据类型为Text类型，通常对应处理后的键信息（比如某个标识等）
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出的值的数据类型为TradeData类型，这是自定义的数据类型，包含具体的业务数据
        job.setMapOutputValueClass(TradeData.class);

        // 设置任务使用的Reducer类，这个类负责对Mapper输出的中间键值对进行归约处理，生成最终结果
        job.setReducerClass(TradeFilterReducer.class);
        // 设置任务最终输出的键的数据类型为Text类型，对应最终结果的键信息
        job.setOutputKeyClass(Text.class);
        // 设置任务最终输出的值的数据类型为Text类型，对应最终结果的值信息
        job.setOutputValueClass(Text.class);

        // 设置任务中Reduce任务的数量为1，可根据实际数据量等情况进行调整，比如数据量很大时可以适当增加
        job.setNumReduceTasks(1);

        // 提交任务并等待任务完成，根据任务完成情况返回相应的退出码（成功返回0，失败返回1）
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // 用于设置并运行第二个MapReduce任务（TradeMerge任务）的私有方法
    private static int runSecondMapReduceJob(String inputPath, String outputPath, int timeWindow)
            throws IOException, InterruptedException, ClassNotFoundException {
        // 创建一个新的Hadoop配置对象，用于设置任务相关的各种配置参数
        Configuration conf = new Configuration();
        // 在配置中设置时间窗口参数，这个参数会传递到MapReduce任务中供相关逻辑使用
        conf.setInt("timeWindow", timeWindow);

        // 根据配置创建一个MapReduce任务实例，任务名称为"TradeMerge"
        Job job = Job.getInstance(conf, "TradeMerge");

        // 设置任务的主类，Hadoop框架会根据这个类来查找相关的资源（如Mapper、Reducer等类）
        job.setJarByClass(TradeController.class);

        // 设置输入数据的路径，将指定的输入路径添加到任务的输入路径列表中，这里使用的是文本格式的输入
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // 设置任务的输入数据格式为文本格式，意味着输入的数据将按文本行进行读取和处理
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出数据的路径，指定任务处理后结果的输出位置，这里使用的是文本格式输出
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 设置任务的输出数据格式为文本格式，意味着输出的数据将按文本格式进行保存
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置任务使用的Mapper类，这个类负责对输入数据进行映射处理，生成中间键值对
        job.setMapperClass(TradeMergeMapper.class);
        // 设置Mapper输出的键的数据类型为Text类型，通常对应处理后的键信息（比如某个标识等）
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出的值的数据类型为Text类型，对应具体的文本内容等业务数据
        job.setMapOutputValueClass(Text.class);

        // 设置任务使用的Reducer类，这个类负责对Mapper输出的中间键值对进行归约处理，生成最终结果
        job.setReducerClass(TradeMergeReducer.class);
        // 设置任务最终输出的键的数据类型为Text类型，对应最终结果的键信息
        job.setOutputKeyClass(Text.class);
        // 设置任务最终输出的值的数据类型为Text类型，对应最终结果的值信息
        job.setOutputValueClass(Text.class);

        // 设置任务中Reduce任务的数量为1，可根据实际数据量等情况进行调整，比如数据量很大时可以适当增加
        job.setNumReduceTasks(1);

        // 提交任务并等待任务完成，根据任务完成情况返回相应的退出码（成功返回0，失败返回1）
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // 用于设置并运行第三个MapReduce任务（TradeConver任务）的私有方法
    private static int runThirdMapReduceJob(String inputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException {
        // 创建一个新的Hadoop配置对象，用于设置任务相关的各种配置参数
        Configuration conf = new Configuration();
        // 根据配置创建一个MapReduce任务实例，任务名称为"TradeConver"
        Job job = Job.getInstance(conf, "TradeConver");

        // 设置任务的主类，Hadoop框架会根据这个类来查找相关的资源（如Mapper、Reducer等类）
        job.setJarByClass(TradeController.class);

        // 设置输入数据的路径，将指定的输入路径添加到任务的输入路径列表中，这里使用的是文本格式的输入
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // 设置任务的输入数据格式为文本格式，意味着输入的数据将按文本行进行读取和处理
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出数据的路径，指定任务处理后结果的输出位置，这里使用的是文本格式输出
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 设置任务的输出数据格式为文本格式，意味着输出的数据将按文本格式进行保存
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置任务使用的Mapper类，这个类负责对输入数据进行映射处理，生成中间键值对
        job.setMapperClass(ConvertMapper.class); // 假设你使用了与前一个任务相同的Mapper
        // 设置Mapper输出的键的数据类型为Text类型，通常对应处理后的键信息（比如某个标识等）
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出的值的数据类型为Text类型，对应具体的文本内容等业务数据
        job.setMapOutputValueClass(Text.class);

        // 设置任务使用的Reducer类，这个类负责对Mapper输出的中间键值对进行归约处理，生成最终结果
        job.setReducerClass(ConverReducer.class); // 使用你的ConverReducer
        // 设置任务最终输出的键的数据类型为Text类型，对应最终结果的键信息
        job.setOutputKeyClass(Text.class);
        // 设置任务最终输出的值的数据类型为Text类型，对应最终结果的值信息
        job.setOutputValueClass(Text.class);

        // 设置任务中Reduce任务的数量为1，可根据实际数据量等情况进行调整，比如数据量很大时可以适当增加
        job.setNumReduceTasks(1);

        // 提交任务并等待任务完成，根据任务完成情况返回相应的退出码（成功返回0，失败返回1）
        return job.waitForCompletion(true) ? 0 : 1;
    }
}