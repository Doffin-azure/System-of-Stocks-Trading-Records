# Stock Trading Record Analysis

A SUSTech STA321 course project that processes stock-trade records, estimates large-trade capital flows, and displays the resulting time series in a local web application.

Start with the [course report](report/STA321%20Project%20Report.pdf) for the input and aggregation method.

1. Convert trade records with the [data preparation notebook](data_convert.ipynb).
2. Filter and aggregate them with the [Java MapReduce jobs](代码/mapreduce/System-of-Stocks-Trading-Records/demo).
3. Open the output in the [Spring Boot CSV visualization](代码/csv-visualization). The [sample summary CSV](price_table_sorted.csv) contains net inflow, buy/sell volume and value by trade size, and time ranges.

The processing and visualization modules are separate Maven projects.
