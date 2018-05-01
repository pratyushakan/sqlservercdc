/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.ing.kafka.connect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ing.kafka.connect.jdbc.util.DateTimeUtils;

public class NetChangesTableQuerier extends TableQuerier {
	private static final Logger log = LoggerFactory.getLogger(NetChangesTableQuerier.class);

	private String timestampColumn;
	private long timestampDelay;
	private TimestampOffset offset;

	public NetChangesTableQuerier(QueryMode mode, String name, String topicPrefix, String timestampColumn,
			Map<String, Object> offsetMap, Long timestampDelay, String schemaPattern, boolean mapNumerics) {
		super(mode, name, topicPrefix, schemaPattern, mapNumerics);
		this.timestampColumn = timestampColumn;
		this.timestampDelay = timestampDelay;
		if(offsetMap != null)
			this.offset = TimestampOffset.fromMap(offsetMap);			
	}

	@Override
	protected void createPreparedStatement(Connection db) throws SQLException {

		StringBuilder builder = new StringBuilder();

		switch (mode) {
		case TABLE:
			builder.append("SELECT *, sys.fn_cdc_map_lsn_to_time(__$start_lsn) as createtime from "
					+ "cdc.fn_cdc_get_net_changes_" + this.name + " ( ");
			if (this.offset == null) {
				builder.append("sys.fn_cdc_get_min_lsn('" + this.name + "')");				
			} else {
				builder.append("sys.fn_cdc_map_time_to_lsn ('smallest greater than', ?)");
			}

			builder.append(", sys.fn_cdc_get_max_lsn(), 'all with mask')");
			break;
		case QUERY:
			builder.append(query);
			break;
		default:
			throw new ConnectException("Unknown mode encountered when preparing query: " + mode);
		}
		String queryString = builder.toString();
		log.debug("{} prepared SQL query: {}", this, queryString);
		stmt = db.prepareStatement(queryString);
	}

	@Override
	protected ResultSet executeQuery() throws SQLException {

		if (this.offset != null) {
			stmt.setTimestamp(1, offset.getTimestampOffset(), DateTimeUtils.UTC_CALENDAR.get());
		} else {
			this.offset = TimestampOffset.fromMap(null);
		}

		return stmt.executeQuery();
	}

	@Override
	public SourceRecord extractRecord() throws SQLException {
		final Struct record = DataConverter.convertRecord(schema, resultSet, mapNumerics);
		offset = extractOffset(schema, record);
		// TODO: Key?
		final String topic;
		final Map<String, String> partition;
		switch (mode) {
		case TABLE:
			partition = Collections.singletonMap(MySourceConnectorConstants.NET_CHANGES_KEY, this.name);
			topic = topicPrefix + "-net-" + this.name.replace('_', '-');
			break;
		case QUERY:
			partition = Collections.singletonMap(MySourceConnectorConstants.QUERY_NAME_KEY,
					MySourceConnectorConstants.QUERY_NAME_VALUE);
			topic = topicPrefix;
			break;
		default:
			throw new ConnectException("Unexpected query mode: " + mode);
		}
		return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
	}

	// Visible for testing
	TimestampOffset extractOffset(Schema schema, Struct record) {
		final Timestamp extractedTimestamp;
		if (timestampColumn != null) {
			extractedTimestamp = (Timestamp) record.get(timestampColumn);
			Timestamp timestampOffset = offset.getTimestampOffset();
			assert timestampOffset != null && timestampOffset.compareTo(extractedTimestamp) <= 0;
		} else {
			extractedTimestamp = null;
		}

		return new TimestampOffset(extractedTimestamp);
	}

	@Override
	public String toString() {
		return "TimestampIncrementingTableQuerier{" + "name='" + name + '\'' + ", query='" + query + '\''
				+ ", topicPrefix='" + topicPrefix + '\'' + ", timestampColumn='" + timestampColumn + '\'' + '}';
	}
}
