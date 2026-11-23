# Stock Trading Record Analysis

A course project for SUSTech's [STA321 Distributed Storage and Parallel Computing](https://mirrors.sustech.edu.cn/courses/syllabus/html/STA321.html). It processes stock-trade records with Hadoop MapReduce, estimates large-trade capital flows, and displays the resulting time series in a local Spring Boot dashboard.

Start with the [course report](report/STA321%20Project%20Report.pdf) for the input and aggregation method.

1. Convert trade records with the [data preparation notebook](data_convert.ipynb).
2. Filter and aggregate them with the [Java MapReduce jobs](代码/mapreduce/System-of-Stocks-Trading-Records/demo).
3. Open the output in the [Spring Boot CSV visualization](代码/csv-visualization). The [sample summary CSV](price_table_sorted.csv) contains net inflow, buy/sell volume and value by trade size, and time ranges.

The processing and visualization modules are separate Maven projects.

## Run the visualization

With JDK 21, run:

```bash
cd 代码/csv-visualization
./mvnw spring-boot:run
```

On Windows, use `.\mvnw.cmd spring-boot:run` from the same module. Open [the local dashboard](http://localhost:8080/) to view the bundled CSV. Enter a start time, an end time, or both in HHmm format (for example, `0930`). The filter compares the end time of each trading interval.
