package com.example.csv_visualization;

public class MyData {
    // 声明各个变量，用来存储不同的股票数据项

    private String timeRange;       // 时间范围，例如：'2024年Q1'
    private double mainInflow;      // 主力净流入，表示主力资金的净流入量
    private double mainInflowAmount; // 主力流入，表示主力资金流入的金额
    private double mainOutflow;     // 主力流出，表示主力资金流出的金额
    private double superBuyVolume;  // 超大买单成交量
    private double superBuyAmount;  // 超大买单成交额
    private double superSellVolume; // 超大卖单成交量
    private double superSellAmount; // 超大卖单成交额
    private double bigBuyVolume;    // 大买单成交量
    private double bigBuyAmount;    // 大买单成交额
    private double bigSellVolume;   // 大卖单成交量
    private double bigSellAmount;   // 大卖单成交额
    private double midBuyVolume;    // 中买单成交量
    private double midBuyAmount;    // 中买单成交额
    private double midSellVolume;   // 中卖单成交量
    private double midSellAmount;   // 中卖单成交额
    private double smallBuyVolume;  // 小买单成交量
    private double smallBuyAmount;  // 小买单成交额
    private double smallSellVolume; // 小卖单成交量
    private double smallSellAmount; // 小卖单成交额

    // 构造函数，初始化 MyData 对象的所有成员变量
    public MyData(double mainInflow, double mainInflowAmount, double mainOutflow,
                  double superBuyVolume, double superBuyAmount, double superSellVolume, double superSellAmount,
                  double bigBuyVolume, double bigBuyAmount, double bigSellVolume, double bigSellAmount,
                  double midBuyVolume, double midBuyAmount, double midSellVolume, double midSellAmount,
                  double smallBuyVolume, double smallBuyAmount, double smallSellVolume, double smallSellAmount, String timeRange) {

        // 初始化每个成员变量的值
        this.mainInflow = mainInflow;
        this.mainInflowAmount = mainInflowAmount;
        this.mainOutflow = mainOutflow;
        this.superBuyVolume = superBuyVolume;
        this.superBuyAmount = superBuyAmount;
        this.superSellVolume = superSellVolume;
        this.superSellAmount = superSellAmount;
        this.bigBuyVolume = bigBuyVolume;
        this.bigBuyAmount = bigBuyAmount;
        this.bigSellVolume = bigSellVolume;
        this.bigSellAmount = bigSellAmount;
        this.midBuyVolume = midBuyVolume;
        this.midBuyAmount = midBuyAmount;
        this.midSellVolume = midSellVolume;
        this.midSellAmount = midSellAmount;
        this.smallBuyVolume = smallBuyVolume;
        this.smallBuyAmount = smallBuyAmount;
        this.smallSellVolume = smallSellVolume;
        this.smallSellAmount = smallSellAmount;
        this.timeRange = timeRange;  // 设置时间范围
    }

    // Getter 和 Setter 方法，用于访问和修改对象的成员变量

    public double getMainInflow() { return mainInflow; }  // 获取主力净流入值
    public void setMainInflow(double mainInflow) { this.mainInflow = mainInflow; }  // 设置主力净流入值

    public double getMainInflowAmount() { return mainInflowAmount; }  // 获取主力流入金额
    public void setMainInflowAmount(double mainInflowAmount) { this.mainInflowAmount = mainInflowAmount; }  // 设置主力流入金额

    public double getMainOutflow() { return mainOutflow; }  // 获取主力流出金额
    public void setMainOutflow(double mainOutflow) { this.mainOutflow = mainOutflow; }  // 设置主力流出金额

    public double getSuperBuyVolume() { return superBuyVolume; }  // 获取超大买单成交量
    public void setSuperBuyVolume(double superBuyVolume) { this.superBuyVolume = superBuyVolume; }  // 设置超大买单成交量

    public double getSuperBuyAmount() { return superBuyAmount; }  // 获取超大买单成交额
    public void setSuperBuyAmount(double superBuyAmount) { this.superBuyAmount = superBuyAmount; }  // 设置超大买单成交额

    public double getSuperSellVolume() { return superSellVolume; }  // 获取超大卖单成交量
    public void setSuperSellVolume(double superSellVolume) { this.superSellVolume = superSellVolume; }  // 设置超大卖单成交量

    public double getSuperSellAmount() { return superSellAmount; }  // 获取超大卖单成交额
    public void setSuperSellAmount(double superSellAmount) { this.superSellAmount = superSellAmount; }  // 设置超大卖单成交额

    public double getBigBuyVolume() { return bigBuyVolume; }  // 获取大买单成交量
    public void setBigBuyVolume(double bigBuyVolume) { this.bigBuyVolume = bigBuyVolume; }  // 设置大买单成交量

    public double getBigBuyAmount() { return bigBuyAmount; }  // 获取大买单成交额
    public void setBigBuyAmount(double bigBuyAmount) { this.bigBuyAmount = bigBuyAmount; }  // 设置大买单成交额

    public double getBigSellVolume() { return bigSellVolume; }  // 获取大卖单成交量
    public void setBigSellVolume(double bigSellVolume) { this.bigSellVolume = bigSellVolume; }  // 设置大卖单成交量

    public double getBigSellAmount() { return bigSellAmount; }  // 获取大卖单成交额
    public void setBigSellAmount(double bigSellAmount) { this.bigSellAmount = bigSellAmount; }  // 设置大卖单成交额

    public double getMidBuyVolume() { return midBuyVolume; }  // 获取中买单成交量
    public void setMidBuyVolume(double midBuyVolume) { this.midBuyVolume = midBuyVolume; }  // 设置中买单成交量

    public double getMidBuyAmount() { return midBuyAmount; }  // 获取中买单成交额
    public void setMidBuyAmount(double midBuyAmount) { this.midBuyAmount = midBuyAmount; }  // 设置中买单成交额

    public double getMidSellVolume() { return midSellVolume; }  // 获取中卖单成交量
    public void setMidSellVolume(double midSellVolume) { this.midSellVolume = midSellVolume; }  // 设置中卖单成交量

    public double getMidSellAmount() { return midSellAmount; }  // 获取中卖单成交额
    public void setMidSellAmount(double midSellAmount) { this.midSellAmount = midSellAmount; }  // 设置中卖单成交额

    public double getSmallBuyVolume() { return smallBuyVolume; }  // 获取小买单成交量
    public void setSmallBuyVolume(double smallBuyVolume) { this.smallBuyVolume = smallBuyVolume; }  // 设置小买单成交量

    public double getSmallBuyAmount() { return smallBuyAmount; }  // 获取小买单成交额
    public void setSmallBuyAmount(double smallBuyAmount) { this.smallBuyAmount = smallBuyAmount; }  // 设置小买单成交额

    public double getSmallSellVolume() { return smallSellVolume; }  // 获取小卖单成交量
    public void setSmallSellVolume(double smallSellVolume) { this.smallSellVolume = smallSellVolume; }  // 设置小卖单成交量

    public double getSmallSellAmount() { return smallSellAmount; }  // 获取小卖单成交额
    public void setSmallSellAmount(double smallSellAmount) { this.smallSellAmount = smallSellAmount; }  // 设置小卖单成交额

    public String getTimeRange() { return timeRange; }  // 获取时间范围
    public void setTimeRange(String timeRange) { this.timeRange = timeRange; }  // 设置时间范围
}
