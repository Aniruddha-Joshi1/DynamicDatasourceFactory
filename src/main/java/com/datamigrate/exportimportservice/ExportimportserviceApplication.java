package com.datamigrate.exportimportservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(exclude = {org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class})
public class ExportimportserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ExportimportserviceApplication.class, args);
	}

}
