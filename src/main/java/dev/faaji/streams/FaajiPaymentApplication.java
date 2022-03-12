package dev.faaji.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan(basePackages = "dev.faaji.streams.config")
public class FaajiPaymentApplication {

	public static void main(String[] args) {
		SpringApplication.run(FaajiPaymentApplication.class, args);
	}

}
