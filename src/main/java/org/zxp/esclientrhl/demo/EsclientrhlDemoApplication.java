package org.zxp.esclientrhl.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.zxp.esclientrhl.annotation.EnableESTools;

@SpringBootApplication
@EnableESTools(basePackages={"org.zxp.esclientrhl.demo.repository"},entityPath = {"org.zxp.esclientrhl.demo.domain"})
public class EsclientrhlDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(EsclientrhlDemoApplication.class, args);
	}

}
