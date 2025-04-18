package com.example.csv_visualization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Controller
public class ChartController {

    @Autowired
    private CsvService csvService; // 自动注入 CsvService 服务，用于处理 CSV 文件

    @GetMapping("/showChart")
    public String showChart(@RequestParam(value = "startTime", required = false) Double startTime,
                            @RequestParam(value = "endTime", required = false) Double endTime,
                            Model model) throws IOException, CsvValidationException {
        // 从指定的 CSV 文件中读取数据
        List<MyData> dataList = csvService.parseCsv("data/output.csv");

        // 初始化一个过滤后的数据列表，默认为完整的数据列表
        List<MyData> filteredDataList = dataList;
        System.out.println(dataList); // 打印读取到的完整数据列表

        // 如果起始时间和结束时间不为空，则根据时间范围过滤数据
        if (startTime != null && endTime != null) {
            filteredDataList = dataList.stream()
                    .filter(data -> {
                        // 提取时间范围的子字符串，并将其转换为 Double 类型
                        double time = Double.parseDouble(data.getTimeRange().substring(data.getTimeRange().length() - 9, data.getTimeRange().length() - 5));
                        return time >= startTime && time <= endTime; // 判断时间是否在范围内
                    })
                    .collect(Collectors.toList());
        }

        // 提取原始的时间范围列表
        List<String> originalTimeRangeList = csvService.extractTimeRange(filteredDataList);

        // 将时间范围列表中的时间部分提取出来并转换为 Double 类型
        List<Double> processedTimeRangeList = originalTimeRangeList.stream()
                .map(time -> time.substring(time.length() - 9, time.length() - 5)) // 提取时间子字符串
                .map(Double::parseDouble) // 转换为 Double
                .collect(Collectors.toList());

        // 提取主力净流入等数据
        List<Double> mainInflowList = csvService.extractMainInflow(filteredDataList);
        List<Double> mainInflowAmountList = csvService.extractMainInflowAmount(filteredDataList);
        List<Double> mainOutflowList = csvService.extractMainOutflow(filteredDataList);
        List<Double> supermainInflowList = csvService.extractSuperMainInflow(filteredDataList);
        List<Double> bigmainInflowList = csvService.extractBigMainInflow(filteredDataList);
        List<Double> midmainInflowList = csvService.extractMidMainInflow(filteredDataList);
        List<Double> smallmainInflowList = csvService.extractSmallMainInflow(filteredDataList);

        // 使用 ObjectMapper 将数据转换为 JSON 格式字符串
        ObjectMapper mapper = new ObjectMapper();
        String timeRangeJson = mapper.writeValueAsString(processedTimeRangeList);
        String mainInflowJson = mapper.writeValueAsString(mainInflowList);
        String mainInflowAmountJson = mapper.writeValueAsString(mainInflowAmountList);
        String mainOutflowJson = mapper.writeValueAsString(mainOutflowList);
        String supermainInflowJson = mapper.writeValueAsString(supermainInflowList);
        String bigmainInflowJson = mapper.writeValueAsString(bigmainInflowList);
        String midmainInflowJson = mapper.writeValueAsString(midmainInflowList);
        String smallmainInflowJson = mapper.writeValueAsString(smallmainInflowList);

        // 获取各个类型的成交额数据，包括买单和卖单
        double[] transactionAmounts = csvService.extractTransactionAmounts(filteredDataList);

        // 获取各个类型的成交量数据，包括买单和卖单
        double[] transactionVolumes = csvService.extractTransactionVolumes(filteredDataList);

        // 将转换后的数据传递到前端模板中
        model.addAttribute("timeRangeJson", timeRangeJson);
        model.addAttribute("mainInflowJson", mainInflowJson);
        model.addAttribute("mainInflowAmountJson", mainInflowAmountJson);
        model.addAttribute("mainOutflowJson", mainOutflowJson);
        model.addAttribute("supermainInflowJson", supermainInflowJson);
        model.addAttribute("bigmainInflowJson", bigmainInflowJson);
        model.addAttribute("midmainInflowJson", midmainInflowJson);
        model.addAttribute("smallmainInflowJson", smallmainInflowJson);

        // 添加成交额数据到模型中
        model.addAttribute("superBuyAmount", transactionAmounts[0]);
        model.addAttribute("bigBuyAmount", transactionAmounts[1]);
        model.addAttribute("midBuyAmount", transactionAmounts[2]);
        model.addAttribute("smallBuyAmount", transactionAmounts[3]);
        model.addAttribute("superSellAmount", transactionAmounts[4]);
        model.addAttribute("bigSellAmount", transactionAmounts[5]);
        model.addAttribute("midSellAmount", transactionAmounts[6]);
        model.addAttribute("smallSellAmount", transactionAmounts[7]);

        // 添加成交量数据到模型中
        model.addAttribute("superBuyVolume", transactionVolumes[0]);
        model.addAttribute("bigBuyVolume", transactionVolumes[1]);
        model.addAttribute("midBuyVolume", transactionVolumes[2]);
        model.addAttribute("smallBuyVolume", transactionVolumes[3]);
        model.addAttribute("superSellVolume", transactionVolumes[4]);
        model.addAttribute("bigSellVolume", transactionVolumes[5]);
        model.addAttribute("midSellVolume", transactionVolumes[6]);
        model.addAttribute("smallSellVolume", transactionVolumes[7]);

        // 如果起始时间或结束时间不为空，则打印到控制台
        if (startTime != null) {
            System.out.println(startTime);
        }
        if (endTime != null) {
            System.out.println(endTime);
        }

        // 打印过滤后的数据列表到控制台
        System.out.println(dataList);

        return "chart.html"; // 返回到 "chart.html" 视图
    }
}
