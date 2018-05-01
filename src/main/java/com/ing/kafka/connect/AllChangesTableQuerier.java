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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ing.kafka.connect.jdbc.util.DateTimeUtils;
import com.ing.kafka.connect.jdbc.util.StringUtils;

public class AllChangesTableQuerier extends TableQuerier {
	private static final Logger log = LoggerFactory.getLogger(AllChangesTableQuerier.class);

	private String timestampColumn;
	private long timestampDelay;
	private TimestampOffset offset;
	private Map<String, String> capturedColumns = new HashMap<String, String>();

	public AllChangesTableQuerier(QueryMode mode, String name, String topicPrefix, String timestampColumn,
			Map<String, Object> offsetMap, Long timestampDelay, String schemaPattern, boolean mapNumerics) {
		super(mode, name, topicPrefix, schemaPattern, mapNumerics);
		this.timestampColumn = timestampColumn;
		this.timestampDelay = timestampDelay;
		if(offsetMap !=  null)
			this.offset = TimestampOffset.fromMap(offsetMap);		
	}
	
	private void retrieveCapturedColumns(Connection db){
		log.debug("Retrieving captured columns for the capture instance " + this.name);
		CallableStatement cStmt;
		ResultSet rs;
		try {
			cStmt = db.prepareCall("{ call sys.sp_cdc_get_captured_columns(?) }");
			cStmt.setString(1, "dbo_product_CDC");
			rs = cStmt.executeQuery();

			while (rs.next()) {
				capturedColumns.put(rs.getString("column_ordinal"), rs.getString("column_name"));
			} 
		} catch (SQLException e) {
			
		}
		finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ignored) {
					// intentionally ignored
				}
			}
			
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException ignored) {
					// intentionally ignored
				}
			}
		}
		log.debug("End of retrieving captured columns for the capture instance " + this.name);		
	}

	@Override
	protected void createPreparedStatement(Connection db) throws SQLException {

		StringBuilder builder = new StringBuilder();
		
		if (this.capturedColumns.isEmpty()) {
			retrieveCapturedColumns(db);
		}

		switch (mode) {
		case TABLE:
			builder.append("SELECT *, sys.fn_cdc_map_lsn_to_time(__$start_lsn) as createtime");
			
			for(Entry<String, String> entry : this.capturedColumns.entrySet()) {
				builder.append(", sys.fn_cdc_is_bit_set(" + entry.getKey() + ", __$update_mask) as 'is_" + entry.getValue() + "_changed'");
			}
			
			builder.append(" from cdc.fn_cdc_get_all_changes_" + this.name + " ( ");
			if (this.offset == null) {
				builder.append("sys.fn_cdc_get_min_lsn('" + this.name + "')");				
			} else {
				builder.append("sys.fn_cdc_map_time_to_lsn ('smallest greater than', ?)");
			}

			builder.append(", sys.fn_cdc_get_max_lsn(), 'all update old')");
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
		}
		else {
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
			partition = Collections.singletonMap(MySourceConnectorConstants.ALL_CHANGES_KEY, this.name);
			topic = topicPrefix + "-all-" + this.name.replace('_', '-');
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
