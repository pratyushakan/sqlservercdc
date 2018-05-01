/**
 * Copyright 2016 Confluent Inc.
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

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class TimestampOffset {
	private static final String TIMESTAMP_FIELD = "timestamp";
	private static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

	private final Timestamp timestampOffset;

	/**
	 * @param timestampOffset
	 *            the timestamp offset. If null, {@link #getTimestampOffset()}
	 *            will return {@code new Timestamp(0)}.
	 */
	public TimestampOffset(Timestamp timestampOffset) {
		this.timestampOffset = timestampOffset;
	}

	public Timestamp getTimestampOffset() {
		return timestampOffset == null ? new Timestamp(0) : timestampOffset;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>(2);
		if (timestampOffset != null) {
			map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
			map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
		}
		return map;
	}

	public static TimestampOffset fromMap(Map<String, ?> map) {
		if (map == null || map.isEmpty()) {
			return new TimestampOffset(null);
		}

		Long millis = (Long) map.get(TIMESTAMP_FIELD);
		Timestamp ts = null;
		if (millis != null) {
			ts = new Timestamp(millis);
			Long nanos = (Long) map.get(TIMESTAMP_NANOS_FIELD);
			if (nanos != null) {
				ts.setNanos(nanos.intValue());
			}
		}
		return new TimestampOffset(ts);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TimestampOffset that = (TimestampOffset) o;

		return timestampOffset != null ? timestampOffset.equals(that.timestampOffset) : that.timestampOffset == null;

	}

	@Override
	public int hashCode() {
		int result = timestampOffset != null ? timestampOffset.hashCode() : 0;
		return result;
	}
}
