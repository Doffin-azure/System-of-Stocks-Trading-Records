import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TradeData implements Writable {
    private IntWritable tradeQty;
    private DoubleWritable price;

    public TradeData() {
        this.tradeQty = new IntWritable();
        this.price = new DoubleWritable();
    }

    public TradeData(int tradeQty, double price) {
        this.tradeQty = new IntWritable(tradeQty);
        this.price = new DoubleWritable(price);
    }

    public int getTradeQty() {
        return tradeQty.get();
    }

    public double getPrice() {
        return price.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tradeQty.write(out);
        price.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tradeQty.readFields(in);
        price.readFields(in);
    }

    @Override
    public String toString() {
        return tradeQty.toString() + "," + price.toString();
    }
}
