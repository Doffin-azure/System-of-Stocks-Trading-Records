package com.example.csv_visualization;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Service
// 定义一个服务类，用于解析 CSV 文件并处理相关数据
public class CsvService {

    // 定义一个方法，解析 CSV 文件，将其转换为数据对象列表
    public List<MyData> parseCsv(String filePath) throws IOException, CsvValidationException {
        List<MyData> dataList = new ArrayList<>(); // 用于存储解析后的数据对象
        try (CSVReader reader = new CSVReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream(filePath)))) {
            String[] nextLine; // 用于逐行读取 CSV 文件
            reader.readNext(); // 跳过第一行表头
            // 使用 CSVReader 和 CsvValidationException 读取 CSV 文件数据
            while ((nextLine = reader.readNext()) != null) { // 判断是否还有数据未读取完
                // 创建 MyData 对象，并从每行数据中解析对应字段
                MyData data = new MyData(
                        Double.parseDouble(nextLine[0]),  // 主力净流入
                        Double.parseDouble(nextLine[1]),  // 主力流入
                        Double.parseDouble(nextLine[2]),  // 主力流出
                        Double.parseDouble(nextLine[3]),  // 超大买单成交量
                        Double.parseDouble(nextLine[4]),  // 超大买单成交额
                        Double.parseDouble(nextLine[5]),  // 超大卖单成交量
                        Double.parseDouble(nextLine[6]),  // 超大卖单成交额
                        Double.parseDouble(nextLine[7]),  // 大买单成交量
                        Double.parseDouble(nextLine[8]),  // 大买单成交额
                        Double.parseDouble(nextLine[9]),  // 大卖单成交量
                        Double.parseDouble(nextLine[10]), // 大卖单成交额
                        Double.parseDouble(nextLine[11]), // 中买单成交量
                        Double.parseDouble(nextLine[12]), // 中买单成交额
                        Double.parseDouble(nextLine[13]), // 中卖单成交量
                        Double.parseDouble(nextLine[14]), // 中卖单成交额
                        Double.parseDouble(nextLine[15]), // 小买单成交量
                        Double.parseDouble(nextLine[16]), // 小买单成交额
                        Double.parseDouble(nextLine[17]), // 小卖单成交量
                        Double.parseDouble(nextLine[18]), // 小卖单成交额
                        nextLine[19].trim()               // 时间范围
                );
                dataList.add(data); // 将解析出的数据对象添加到列表中
            }
        }
        return dataList; // 返回解析后的数据列表
    }

    // 提取时间范围列表
    public List<String> extractTimeRange(List<MyData> dataList) {
        List<String> timeRangeList = new ArrayList<>();
        for (MyData data : dataList) {
            timeRangeList.add(data.getTimeRange()); // 获取每条数据的时间范围
        }
        return timeRangeList; // 返回时间范围列表
    }

    // 提取主力净流入数据
    public List<Double> extractMainInflow(List<MyData> dataList) {
        List<Double> mainInflowList = new ArrayList<>();
        for (MyData data : dataList) {
            mainInflowList.add(data.getMainInflow()); // 获取主力净流入数据
        }
        return mainInflowList; // 返回主力净流入数据列表
    }

    // 提取主力流入数据
    public List<Double> extractMainInflowAmount(List<MyData> dataList) {
        List<Double> mainInflowAmountList = new ArrayList<>();
        for (MyData data : dataList) {
            mainInflowAmountList.add(data.getMainInflowAmount()); // 获取主力流入数据
        }
        return mainInflowAmountList; // 返回主力流入数据列表
    }

    // 提取主力流出数据
    public List<Double> extractMainOutflow(List<MyData> dataList) {
        List<Double> mainOutflowList = new ArrayList<>();
        for (MyData data : dataList) {
            mainOutflowList.add(data.getMainOutflow()); // 获取主力流出数据
        }
        return mainOutflowList; // 返回主力流出数据列表
    }

    // 提取超大买单净流入
    public List<Double> extractSuperMainInflow(List<MyData> dataList) {
        List<Double> supermainInflowList = new ArrayList<>();
        for (MyData data : dataList) {
            supermainInflowList.add(data.getSuperBuyAmount() - data.getSuperSellAmount()); // 计算超大买单净流入
        }
        return supermainInflowList; // 返回超大买单净流入数据列表
    }

    // 提取大单净流入
    public List<Double> extractBigMainInflow(List<MyData> dataList) {
        List<Double> bigmainInflowList = new ArrayList<>();
        for (MyData data : dataList) {
            bigmainInflowList.add(data.getBigBuyAmount() - data.getBigSellAmount()); // 计算大单净流入
        }
        return bigmainInflowList; // 返回大单净流入数据列表
    }

    // 提取中单净流入
    public List<Double> extractMidMainInflow(List<MyData> dataList) {
        List<Double> midmainInflowList = new ArrayList<>();
        for (MyData data : dataList) {
            midmainInflowList.add(data.getMidBuyAmount() - data.getMidSellAmount()); // 计算中单净流入
        }
        return midmainInflowList; // 返回中单净流入数据列表
    }

    // 提取小单净流入
    public List<Double> extractSmallMainInflow(List<MyData> dataList) {
        List<Double> smallmainInflowList = new ArrayList<>();
        for (MyData data : dataList) {
            smallmainInflowList.add(data.getSmallBuyAmount() - data.getSmallSellAmount()); // 计算小单净流入
        }
        return smallmainInflowList; // 返回小单净流入数据列表
    }

    // 提取各种成交额数据
    public double[] extractTransactionAmounts(List<MyData> dataList) {
        double superBuyAmount = 0, bigBuyAmount = 0, midBuyAmount = 0, smallBuyAmount = 0; // 定义变量存储买单数据
        double superSellAmount = 0, bigSellAmount = 0, midSellAmount = 0, smallSellAmount = 0; // 定义变量存储卖单数据

        for (MyData data : dataList) {
            superBuyAmount += data.getSuperBuyAmount();//对于总买卖单数据，累加算出总和
            bigBuyAmount += data.getBigBuyAmount();//对于总买卖单数据，累加算出总和
            midBuyAmount += data.getMidBuyAmount();//对于总买卖单数据，累加算出总和
            smallBuyAmount += data.getSmallBuyAmount();//对于总买卖单数据，累加算出总和

            superSellAmount += data.getSuperSellAmount();//对于总买卖单数据，累加算出总和
            bigSellAmount += data.getBigSellAmount();//对于总买卖单数据，累加算出总和
            midSellAmount += data.getMidSellAmount();//对于总买卖单数据，累加算出总和
            smallSellAmount += data.getSmallSellAmount();//对于总买卖单数据，累加算出总和
        }

        // 返回买单和卖单的成交额数组
        return new double[]{superBuyAmount, bigBuyAmount, midBuyAmount, smallBuyAmount,
                superSellAmount, bigSellAmount, midSellAmount, smallSellAmount};
    }

    // 提取各种成交量数据
    public double[] extractTransactionVolumes(List<MyData> dataList) {
        double superBuyVolume = 0, bigBuyVolume = 0, midBuyVolume = 0, smallBuyVolume = 0; // 定义变量存储买单数据
        double superSellVolume = 0, bigSellVolume = 0, midSellVolume = 0, smallSellVolume = 0; // 定义变量存储卖单数据

        for (MyData data : dataList) {
            superBuyVolume += data.getSuperBuyVolume();//对于总买卖单数据，累加算出总和
            bigBuyVolume += data.getBigBuyVolume();//对于总买卖单数据，累加算出总和
            midBuyVolume += data.getMidBuyVolume();//对于总买卖单数据，累加算出总和
            smallBuyVolume += data.getSmallBuyVolume();//对于总买卖单数据，累加算出总和

            superSellVolume += data.getSuperSellVolume();//对于总买卖单数据，累加算出总和
            bigSellVolume += data.getBigSellVolume();//对于总买卖单数据，累加算出总和
            midSellVolume += data.getMidSellVolume();//对于总买卖单数据，累加算出总和
            smallSellVolume += data.getSmallSellVolume();//对于总买卖单数据，累加算出总和
        }

        // 返回买单和卖单的成交量数组
        return new double[]{superBuyVolume, bigBuyVolume, midBuyVolume, smallBuyVolume,
                superSellVolume, bigSellVolume, midSellVolume, smallSellVolume};
    }
}
