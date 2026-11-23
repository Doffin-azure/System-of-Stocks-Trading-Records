package com.example.csv_visualization;

import com.opencsv.exceptions.CsvValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.List;

@Controller
public class ChartController {
    private static final DateTimeFormatter TRADE_TIMESTAMP = DateTimeFormatter
            .ofPattern("uuuuMMddHHmmssSSS").withResolverStyle(ResolverStyle.STRICT);
    private final CsvService csvService;

    public ChartController(CsvService csvService) {
        this.csvService = csvService;
    }

    @GetMapping({"/", "/showChart"})
    public String showChart(@RequestParam(value = "startTime", required = false) Integer startTime,
                            @RequestParam(value = "endTime", required = false) Integer endTime,
                            Model model) throws IOException, CsvValidationException {
        if (!isValidTime(startTime) || !isValidTime(endTime)
                || (startTime != null && endTime != null && startTime > endTime)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Enter a valid HHmm time range");
        }

        List<MyData> data = csvService.parseCsv("data/output.csv").stream()
                .filter(row -> inTimeRange(row, startTime, endTime))
                .toList();

        model.addAttribute("timeRange", data.stream().map(ChartController::bucketEnd).toList());
        model.addAttribute("mainInflow", csvService.extractMainInflow(data));
        model.addAttribute("mainInflowAmount", csvService.extractMainInflowAmount(data));
        model.addAttribute("mainOutflow", csvService.extractMainOutflow(data));
        model.addAttribute("supermainInflow", csvService.extractSuperMainInflow(data));
        model.addAttribute("bigmainInflow", csvService.extractBigMainInflow(data));
        model.addAttribute("midmainInflow", csvService.extractMidMainInflow(data));
        model.addAttribute("smallmainInflow", csvService.extractSmallMainInflow(data));

        double[] amounts = csvService.extractTransactionAmounts(data);
        model.addAttribute("superBuyAmount", amounts[0]);
        model.addAttribute("bigBuyAmount", amounts[1]);
        model.addAttribute("midBuyAmount", amounts[2]);
        model.addAttribute("smallBuyAmount", amounts[3]);
        model.addAttribute("superSellAmount", amounts[4]);
        model.addAttribute("bigSellAmount", amounts[5]);
        model.addAttribute("midSellAmount", amounts[6]);
        model.addAttribute("smallSellAmount", amounts[7]);

        double[] volumes = csvService.extractTransactionVolumes(data);
        model.addAttribute("superBuyVolume", volumes[0]);
        model.addAttribute("bigBuyVolume", volumes[1]);
        model.addAttribute("midBuyVolume", volumes[2]);
        model.addAttribute("smallBuyVolume", volumes[3]);
        model.addAttribute("superSellVolume", volumes[4]);
        model.addAttribute("bigSellVolume", volumes[5]);
        model.addAttribute("midSellVolume", volumes[6]);
        model.addAttribute("smallSellVolume", volumes[7]);

        return "chart";
    }

    private static boolean isValidTime(Integer time) {
        return time == null || (time >= 0 && time <= 2359 && time % 100 < 60);
    }

    private static boolean inTimeRange(MyData row, Integer startTime, Integer endTime) {
        int time = bucketEnd(row);
        return (startTime == null || time >= startTime) && (endTime == null || time <= endTime);
    }

    private static int bucketEnd(MyData row) {
        String range = row.getTimeRange();
        int separator = range.indexOf(" to ");
        if (separator < 0) {
            throw new IllegalArgumentException("Invalid trade interval: " + range);
        }
        LocalDateTime end = LocalDateTime.parse(range.substring(separator + " to ".length()), TRADE_TIMESTAMP);
        return end.getHour() * 100 + end.getMinute();
    }
}
