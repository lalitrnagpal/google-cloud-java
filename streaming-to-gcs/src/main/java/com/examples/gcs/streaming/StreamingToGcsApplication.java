package com.examples.gcs.streaming;

import java.util.Date;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.stream.IntStream;

@SpringBootApplication
public class StreamingToGcsApplication {

	public static void main(String[] args) {
		SpringApplication.run(StreamingToGcsApplication.class, args);
		StreamingFiles sFiles = new StreamingFiles();
		sFiles.authenticate();
		

		IntStream.range(0, 48).forEachOrdered(i -> {
			long startTime = 0l, endTime = 0l;
			startTime = (new Date()).getTime();
			sFiles.downloadFile("perf-tests-two", "aes-small.csv");
			endTime = (new Date()).getTime();
			System.out.println("Time taken for download (seconds) -> " + (startTime - endTime) / 1000);
			
			startTime = (new Date()).getTime();
			sFiles.uploadFile("perf-tests-two", "d://aes.csv", "aes-small.csv");
			endTime = (new Date()).getTime();
			System.out.println("Time taken for upload (seconds) -> " + (startTime - endTime) / 1000);
			
		});
	}

}
        