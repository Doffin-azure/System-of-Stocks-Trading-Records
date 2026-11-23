package com.example.csv_visualization;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CsvVisualizationApplicationTests {
    @Autowired
    private TestRestTemplate http;

    @Test
    void rendersBundledDataOnHomePage() {
        var response = http.getForEntity("/", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("const timeRange = [940,950,1000,");
    }

    @Test
    void acceptsEitherTimeBoundAndIncludesTheEndpoints() {
        assertThat(http.getForObject("/showChart?endTime=0950", String.class))
                .contains("const timeRange = [940,950];");
        assertThat(http.getForObject("/showChart?startTime=1450", String.class))
                .contains("const timeRange = [1450,1500];");
        assertThat(http.getForObject("/showChart?startTime=0950&endTime=0950", String.class))
                .contains("const timeRange = [950];");
    }

    @Test
    void rejectsInvalidTimeRanges() {
        assertThat(http.getForEntity("/showChart?startTime=0960", String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/showChart?startTime=1000&endTime=0950", String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
