package util;

import org.apache.hadoop.io.*;
import java.io.*;

// TradeData类实现了Hadoop的Writable接口，用于在Hadoop的MapReduce框架中进行数据的序列化和反序列化操作，方便数据在不同节点间传输以及持久化存储等。
// 这里它主要用于封装交易相关的数据，包含交易数量和价格两个属性。
public class TradeData implements Writable {

    // 使用Hadoop提供的IntWritable类型来存储交易数量，方便在Hadoop生态下进行序列化等处理，它包装了基本的整数类型。
    private IntWritable tradeQty;
    // 使用Hadoop提供的DoubleWritable类型来存储交易价格，同样便于进行序列化相关操作，包装了基本的双精度浮点型数据。
    private DoubleWritable price;

    // 默认构造函数，用于初始化TradeData对象，创建新的IntWritable和DoubleWritable实例，初始值为默认值（整数默认为0，双精度浮点数默认为0.0）。
    public TradeData() {
        this.tradeQty = new IntWritable();
        this.price = new DoubleWritable();
    }

    // 带参数的构造函数，用于根据传入的交易数量和价格值来初始化TradeData对象，将传入的整数和双精度浮点数分别包装到对应的IntWritable和DoubleWritable实例中。
    public TradeData(int tradeQty, double price) {
        this.tradeQty = new IntWritable(tradeQty);
        this.price = new DoubleWritable(price);
    }

    // 获取交易数量的方法，通过调用IntWritable的get方法返回其包装的整数类型的交易数量值。
    public int getTradeQty() {
        return tradeQty.get();
    }

    // 获取交易价格的方法，通过调用DoubleWritable的get方法返回其包装的双精度浮点型的交易价格值。
    public double getPrice() {
        return price.get();
    }

    // 实现Writable接口的write方法，用于将对象的数据序列化到输出流中，按照顺序先序列化交易数量，再序列化交易价格，这样在反序列化时能按照相同顺序还原数据。
    @Override
    public void write(DataOutput out) throws IOException {
        tradeQty.write(out);
        price.write(out);
    }

    // 实现Writable接口的readFields方法，用于从输入流中反序列化数据来填充对象的属性，按照与write方法相对应的顺序，先读取交易数量，再读取交易价格，从而还原对象的状态。
    @Override
    public void readFields(DataInput in) throws IOException {
        tradeQty.readFields(in);
        price.readFields(in);
    }

    // 重写toString方法，用于方便地将TradeData对象转换为字符串表示形式，这里按照交易数量和价格的字符串表示形式以逗号分隔进行拼接，便于查看和输出对象包含的数据内容。
    @Override
    public String toString() {
        return tradeQty.toString() + "," + price.toString();
    }
}