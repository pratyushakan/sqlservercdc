package com.ing.kafka.connect;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ing.kafka.connect.jdbc.util.CachedConnectionProvider;

public class MySourceTask extends SourceTask {
	static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
	private Time time;
	private MySourceTaskConfig config;
	private CachedConnectionProvider cachedConnectionProvider;
	private AtomicBoolean stop;
	private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();

	public MySourceTask() {
		this.time = new SystemTime();
	}

	public MySourceTask(Time time) {
		this.time = time;
	}

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		try {

			config = new MySourceTaskConfig(properties);
		} catch (ConfigException e) {
			throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
		}

		createConnectionProvider();

		List<String> tables = config.getList(MySourceTaskConfig.TABLES_CONFIG);

		if (tables.isEmpty()) {
			throw new ConnectException(
					"Invalid configuration: each JdbcSourceTask must have at " + "least one table assigned to it");
		}

		String mode = config.getString(MySourceTaskConfig.MODE_CONFIG);
		TableQuerier.QueryMode queryMode = TableQuerier.QueryMode.TABLE;
		String topicPrefix = config.getString(MySourceTaskConfig.TOPIC_PREFIX_CONFIG);
		String schemaPattern = config.getString(MySourceTaskConfig.SCHEMA_PATTERN_CONFIG);
		Long timestampDelayInterval = config.getLong(MySourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
		String timestampColumn = config.getString(MySourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
		boolean allChanges = config.getBoolean(MySourceTaskConfig.CDC_ALL_CHANGES);
		boolean netChanges = config.getBoolean(MySourceTaskConfig.CDC_NET_CHANGES);
		

		boolean mapNumerics = config.getBoolean(MySourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG);

		Map<Map<String, String>, Map<String, Object>> offsets = null;
		if (mode.equals(MySourceTaskConfig.MODE_TIMESTAMP)) {
			List<Map<String, String>> partitions = new ArrayList<>(tables.size());
			switch (queryMode) {
			case TABLE:
				for (String table : tables) {
					if (allChanges) {
						partitions.add(Collections
							.singletonMap(MySourceConnectorConstants.ALL_CHANGES_KEY, table));
					}
					if (netChanges) {
						partitions.add(Collections
								.singletonMap(MySourceConnectorConstants.NET_CHANGES_KEY, table));
					}
					
				}
				break;
			default:
				throw new ConnectException("Unknown query mode: " + queryMode);
			}
			offsets = context.offsetStorageReader().offsets(partitions);
		}

		for(String tableOrQuery : tables) {
			
			if(allChanges) {
				Map<String, String> partition = Collections.singletonMap(MySourceConnectorConstants.ALL_CHANGES_KEY,
						tableOrQuery);
				Map<String, Object> offset = offsets == null ? null : offsets.get(partition);
				tableQueue.add(new AllChangesTableQuerier(queryMode, tableOrQuery, topicPrefix, timestampColumn, offset,
						timestampDelayInterval, schemaPattern, mapNumerics));				
			}
			
			if(netChanges) {
				Map<String, String> partition = Collections.singletonMap(MySourceConnectorConstants.NET_CHANGES_KEY,
						tableOrQuery);
				Map<String, Object> offset = offsets == null ? null : offsets.get(partition);
				tableQueue.add(new NetChangesTableQuerier(queryMode, tableOrQuery, topicPrefix, timestampColumn, offset,
						timestampDelayInterval, schemaPattern, mapNumerics));				
			}			
		}
		
		stop = new AtomicBoolean(false);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		log.trace("{} Polling for new data");

		while (!stop.get()) {
			final TableQuerier querier = tableQueue.peek();
			if (!querier.querying()) {
				// If not in the middle of an update, wait for next update time
				final long nextUpdate = querier.getLastUpdate()
						+ config.getInt(MySourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
				final long untilNext = nextUpdate - time.milliseconds();
				if (untilNext > 0) {
					log.trace("Waiting {} ms to poll {} next", untilNext, querier.toString());
					time.sleep(untilNext);
					continue; // Re-check stop flag before continuing
				}
			}

			final List<SourceRecord> results = new ArrayList<>();
			try {
				log.debug("Checking for next block of results from {}", querier.toString());
				querier.maybeStartQuery(cachedConnectionProvider.getValidConnection());

				int batchMaxRows = config.getInt(MySourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
				boolean hadNext = true;
				while (results.size() < batchMaxRows && (hadNext = querier.next())) {
					results.add(querier.extractRecord());
				}
				
				 if (!hadNext) {
					 resetQuerier(querier);
			      }

				if (results.isEmpty()) {
					log.trace("No updates for {}", querier.toString());
					continue;
				}

				log.debug("Returning {} records for {}", results.size(), querier.toString());
				return results;
			} catch (SQLException e) {
				log.error("Failed to run query for table {}: {}", querier.toString(), e);
				resetQuerier(querier);
				return null;
			}
		}

		// Only in case of shutdown
		return null;
	}

	@Override
	public void stop() {
		if (stop != null) {
			stop.set(true);
		}
		if (cachedConnectionProvider != null) {
			cachedConnectionProvider.closeQuietly();
		}
	}

	private void createConnectionProvider() {
		final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
		final String dbUser = config.getString(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG);
		final Password dbPassword = config.getPassword(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
		final int maxConnectionAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
		final long connectionRetryBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);
		cachedConnectionProvider = new CachedConnectionProvider(dbUrl, dbUser,
				dbPassword == null ? null : dbPassword.value(), maxConnectionAttempts, connectionRetryBackoff);
	}
	
	private void resetQuerier(TableQuerier expectedHead) {
		log.debug("Resetting querier {}", expectedHead.toString());
	    TableQuerier removedQuerier = tableQueue.poll();
	    assert removedQuerier == expectedHead;
	    expectedHead.reset(time.milliseconds());
	    tableQueue.add(expectedHead);  
	  }
}