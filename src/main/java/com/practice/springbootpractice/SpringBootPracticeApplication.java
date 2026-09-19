package com.practice.springbootpractice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootPracticeApplication {

	public static void main(String[] args) {
		// Must be set before any Kafka/Kerberos client classes load.
		System.setProperty("java.security.krb5.conf", "docker/kerberos/krb5-client.conf");
		SpringApplication.run(SpringBootPracticeApplication.class, args);
	}

}
