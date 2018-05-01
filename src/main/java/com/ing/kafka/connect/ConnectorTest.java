package com.ing.kafka.connect;

import java.util.HashMap;
import java.util.Map;

public class ConnectorTest {

	public static void main(String[] args) {
		Map<String, String> properties = new HashMap<String, String>();
		properties.put("connection.url", "jdbc:sqlserver://localhost:1433;database=core");
		properties.put("connection.user", "sa");
		properties.put("connection.password", "admin123");
		properties.put("connection.attempts", "3");
		properties.put("connection.backoff.ms", "3");
		properties.put("mode", "timestamp");
		properties.put("topic.prefix", "test1");
		properties.put("poll.interval.ms", "5000");
		properties.put("table.poll.interval.ms", "10000");
		properties.put("tables", "dbo_product_CDC");
		properties.put("timestamp.column.name", "createtime");
		properties.put("cdc.netchanges", "true");
		properties.put("cdc.allchanges", "false");

		MySourceTask task = new MySourceTask();
		task.start(properties);
		try {
			task.poll();
			task.stop();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
