package com.ing.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ing.kafka.connect.jdbc.util.CachedConnectionProvider;
import com.ing.kafka.connect.jdbc.util.StringUtils;

public class MySourceConnector extends SourceConnector {
	private static Logger log = LoggerFactory.getLogger(MySourceConnector.class);
	private Map<String, String> configProperties;
	private JdbcSourceConnectorConfig config;
	private CachedConnectionProvider cachedConnectionProvider;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		try {

			config = new JdbcSourceConnectorConfig(map);

			configProperties = map;
			config = new JdbcSourceConnectorConfig(configProperties);
		} catch (ConfigException e) {
			throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration " + "error", e);
		}

		final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
		final String dbUser = config.getString(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG);
		final Password dbPassword = config.getPassword(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
		final int maxConnectionAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
		final long connectionRetryBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);
		cachedConnectionProvider = new CachedConnectionProvider(dbUrl, dbUser,
				dbPassword == null ? null : dbPassword.value(), maxConnectionAttempts, connectionRetryBackoff);

		// Initial connection attempt
		cachedConnectionProvider.getValidConnection();
	}

	@Override
	public Class<? extends Task> taskClass() {
		// TODO: Return your task implementation.
		return MySourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		// Hard coding the table name, need to be removed and sent it from the
		// config
		//List<String> currentTables = Arrays.asList("dbo_product_CDC");
		List<String> currentTables = config.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
		int numGroups = Math.min(currentTables.size(), maxTasks);
		List<List<String>> tablesGrouped = ConnectorUtils.groupPartitions(currentTables, numGroups);
		List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
		for (List<String> taskTables : tablesGrouped) {
			Map<String, String> taskProps = new HashMap<>(configProperties);
			taskProps.put(MySourceTaskConfig.TABLES_CONFIG, StringUtils.join(taskTables, ","));
			taskConfigs.add(taskProps);
		}
		return taskConfigs;

	}

	@Override
	public void stop() {
		cachedConnectionProvider.closeQuietly();
	}

	@Override
	public ConfigDef config() {
		return JdbcSourceConnectorConfig.CONFIG_DEF;
	}
}
