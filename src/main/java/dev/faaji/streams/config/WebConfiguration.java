package dev.faaji.streams.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebConfiguration {

    private final FaajiProperties faajiProperties;

    public WebConfiguration(FaajiProperties faajiProperties) {
        this.faajiProperties = faajiProperties;
    }

    @Bean
    public WebClient faajiWebClient() {
        return WebClient.builder()
                .baseUrl(faajiProperties.api().url())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
}
