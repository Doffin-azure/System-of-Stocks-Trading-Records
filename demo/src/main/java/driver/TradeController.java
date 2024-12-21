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

public class TradeController {

    public static void main(String[] args) throws Exception {
        int securityID = 1;
        int timeWindow = 10;
        long allStock = 17170245800L;
        try{
        securityID = Integer.parseInt(args[4]);
        timeWindow = Integer.parseInt(args[5]);
        }catch(Exception e){
            System.out.println("Please input the securityID and timeWindow");
            
        }
    
        // Step 1: 设置并执行第一个MapReduce任务（TradeFilter任务）
        int exitCode = runFirstMapReduceJob(args[0], args[1],securityID);
        if (exitCode != 0) {
            System.err.println("First MapReduce job failed!");
            System.exit(exitCode);
        }

        // Step 2: 设置并执行第二个MapReduce任务（TradeMerge任务）
        exitCode = runSecondMapReduceJob(args[1], args[2],timeWindow);
        if (exitCode != 0) {
            System.err.println("Second MapReduce job failed!");
            System.exit(exitCode);
        }

        // Step 3: 添加并执行第三个MapReduce任务（Conver任务）
        exitCode = runThirdMapReduceJob(args[2], args[3]);  // 添加第三个作业
        if (exitCode != 0) {
            System.err.println("Third MapReduce job failed!");
            System.exit(exitCode);
        }

        

        System.exit(0);
    }

    private static int runFirstMapReduceJob(String inputPath, String outputPath,int securityID) throws IOException, InterruptedException, ClassNotFoundException {
        // 设置作业的配置
        Configuration conf = new Configuration();
        conf.setInt("securityID", securityID);

        Job job = Job.getInstance(conf, "TradeFilter");

        job.setJarByClass(TradeController.class); 

        // 输入数据格式
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        // 输出数据格式
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper 设置
        job.setMapperClass(TradeFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TradeData.class);

        // Reducer 设置
        job.setReducerClass(TradeFilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置任务的其他属性
        job.setNumReduceTasks(1);  // 根据数据量可以调整这个值

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static int runSecondMapReduceJob(String inputPath, String outputPath,int timeWindow) throws IOException, InterruptedException, ClassNotFoundException {
        // 设置作业的配置
        Configuration conf = new Configuration();
        conf.setInt("timeWindow", timeWindow);

        Job job = Job.getInstance(conf, "TradeMerge");

        job.setJarByClass(TradeController.class);

        // 输入数据格式
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        // 输出数据格式
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper 设置
        job.setMapperClass(TradeMergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer 设置
        job.setReducerClass(TradeMergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置任务的其他属性
        job.setNumReduceTasks(1);  // 根据数据量可以调整这个值

        return job.waitForCompletion(true) ? 0 : 1;
    }


    private static int runThirdMapReduceJob(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        // 设置作业的配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TradeConver");

        job.setJarByClass(TradeController.class);

        // 输入数据格式
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        // 输出数据格式
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper 设置
        job.setMapperClass(ConvertMapper.class); // 假设你使用了与前一个任务相同的Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer 设置
        job.setReducerClass(ConverReducer.class);  // 使用你的ConverReducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置任务的其他属性
        job.setNumReduceTasks(1);  // 根据数据量可以调整这个值

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
