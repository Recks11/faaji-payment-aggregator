package dev.faaji.streams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

@ConstructorBinding
@ConfigurationProperties(prefix = "faaji")
public record FaajiProperties(ApiProperties api) {
    public record ApiProperties(String url) {
    }
}
