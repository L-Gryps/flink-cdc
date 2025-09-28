/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.xugu.source.converter;

import org.apache.flink.cdc.connectors.xugu.source.config.XuGuConnectorConfig;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** JdbcValueConverters for OceanBase. */
public class XuGuValueConverters extends JdbcValueConverters {

    public static final String EMPTY_BLOB_FUNCTION = "EMPTY_BLOB()";
    public static final String EMPTY_CLOB_FUNCTION = "EMPTY_CLOB()";

    private static final Pattern TIME_FIELD_PATTERN =
            Pattern.compile(
                    "^(-?(?:[0-1]?\\d|2[0-3])):([0-5]\\d)(?::([0-5]\\d))?(?:\\.(\\d{1,6}))?$");
    //    private static final Pattern TIME_FIELD_PATTERN =
    // Pattern.compile("(\\-?[0-9]*):([0-9]*)(:([0-9]*))?(\\.([0-9]*))?");

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("dd-MMM-yy hh.mm.ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .appendPattern(" a")
                    .toFormatter(Locale.ENGLISH);

    private final String serverTimezone;

    public XuGuValueConverters(XuGuConnectorConfig connectorConfig) {
        super(
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                x -> x,
                BigIntUnsignedMode.PRECISE,
                connectorConfig.binaryHandlingMode());
        this.serverTimezone = connectorConfig.getServerTimeZone();
    }

    @Override
    protected int getTimePrecision(Column column) {
        return super.getTimePrecision(column);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug(
                "Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale());

        switch (column.jdbcType()) {
            case Types.BOOLEAN:
                return SchemaBuilder.bool();
            case Types.TINYINT:
            case Types.SMALLINT:
                return SchemaBuilder.int16();
            case Types.INTEGER:
                return SchemaBuilder.int32();
            case Types.BIGINT:
                return SchemaBuilder.int64();
            case Types.FLOAT:
            case Types.DOUBLE:
                return SchemaBuilder.float64();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return SpecialValueDecimal.builder(
                        decimalMode, column.length(), column.scale().get());
            case Types.DATE:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return io.debezium.time.Date.builder();
                }
                return org.apache.kafka.connect.data.Date.builder();
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return io.debezium.time.MicroTime.builder();
                }
                if (adaptiveTimePrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return io.debezium.time.Time.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return io.debezium.time.MicroTime.builder();
                    }
                    return io.debezium.time.NanoTime.builder();
                }
                return org.apache.kafka.connect.data.Time.builder();
            case Types.TIMESTAMP:
                return getTimestampSchema(column);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.CLOB:
                return SchemaBuilder.string();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
                return binaryMode.getSchema();
            default:
                return super.schemaBuilder(column);
        }
    }

    protected SchemaBuilder getTimestampSchema(Column column) {
        if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
            if (getTimePrecision(column) <= 3) {
                return Timestamp.builder();
            }
            if (getTimePrecision(column) <= 6) {
                return MicroTimestamp.builder();
            }
            return NanoTimestamp.builder();
        }
        return org.apache.kafka.connect.data.Timestamp.builder();
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.BOOLEAN:
                return (data) -> convertBoolean(column, fieldDefn, data);
            case Types.TINYINT:
                return data -> convertTinyInt(column, fieldDefn, data);
            case Types.SMALLINT:
                return data -> convertSmallInt(column, fieldDefn, data);
            case Types.INTEGER:
                return data -> convertInteger(column, fieldDefn, data);
            case Types.BIGINT:
                return (data) -> convertBigInt(column, fieldDefn, data);
            case Types.FLOAT:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.NUMERIC:
            case Types.DECIMAL:
                return data -> convertDecimal(column, fieldDefn, data);
            case Types.DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.DATE:
                return (data) -> convertDate(column, fieldDefn, data);
            case Types.TIME:
                return (data) -> convertTime(column, fieldDefn, data);
            case Types.TIMESTAMP:
                return data -> convertTimestamp(column, fieldDefn, data);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.CLOB:
                return data -> convertString(column, fieldDefn, data);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
                return (data) -> convertBinary(column, fieldDefn, data, binaryMode);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    @Override
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data instanceof BigInteger) {
            data = toBigDecimal(column, fieldDefn, data.toString());
        } else if (data instanceof String) {
            data = toBigDecimal(column, fieldDefn, data);
        }
        if (data instanceof BigDecimal) {
            data = withScaleAdjustedIfNeeded(column, (BigDecimal) data);
        }

        return super.convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected BigDecimal withScaleAdjustedIfNeeded(Column column, BigDecimal data) {
        // deal with Oracle negative scales
        if (column.scale().isPresent() && column.scale().get() < data.scale()) {
            data = data.setScale(column.scale().get());
        }
        return super.withScaleAdjustedIfNeeded(column, data);
    }

    @Override
    protected Object convertBoolean(Column column, Field fieldDefn, Object data) {
        if (data instanceof BigDecimal) {
            return ((BigDecimal) data).byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        if (data instanceof String) {
            return ((String) data) == "true" ? Boolean.TRUE : Boolean.FALSE;
        }
        return super.convertBoolean(column, fieldDefn, data);
    }

    protected Object convertDate(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Date.valueOf(((String) data).trim());
        }
        if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
            return convertDateToEpochDays(column, fieldDefn, data);
        }
        return convertDateToEpochDaysAsDate(column, fieldDefn, data);
    }

    protected Object convertTimestamp(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = java.sql.Timestamp.valueOf(((String) data).trim());
        }
        if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
            if (getTimePrecision(column) <= 3) {
                return convertTimestampToEpochMillis(column, fieldDefn, data);
            }
            if (getTimePrecision(column) <= 6) {
                return convertTimestampToEpochMicros(column, fieldDefn, data);
            }
            return convertTimestampToEpochNanos(column, fieldDefn, data);
        }
        return convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
    }

    protected Instant resolveTimestampStringAsInstant(String dateText) {
        LocalDateTime dateTime;
        if (dateText.indexOf(" AM") > 0 || dateText.indexOf(" PM") > 0) {
            dateTime = LocalDateTime.from(TIMESTAMP_AM_PM_SHORT_FORMATTER.parse(dateText.trim()));
        } else {
            dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(dateText.trim()));
        }
        return dateTime.atZone(ZoneId.of(serverTimezone)).toInstant();
    }

    @Override
    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            String timeStr = (String) data;
            LocalTime localTime = LocalTime.parse(timeStr);
            data = Time.valueOf(localTime);
        }
        // xugu time类型处理负时间值，转换为有效值
        if (data instanceof Time) {
            Time time = (Time) data;
            data = Time.valueOf(time.toString());
        }
        return super.convertTime(column, fieldDefn, data);
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof Clob) {
            try {
                Clob clob = (Clob) data;
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }
        if (data instanceof String) {
            String s = (String) data;
            if (EMPTY_CLOB_FUNCTION.equals(s)) {
                return column.isOptional() ? null : "";
            }
        }
        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertBinary(
            Column column,
            Field fieldDefn,
            Object data,
            CommonConnectorConfig.BinaryHandlingMode mode) {
        try {
            if (data instanceof ByteArrayInputStream) {
                ByteArrayInputStream bis = (ByteArrayInputStream) data;
                data = bis.readAllBytes();
            }

            if (data instanceof Blob) {
                Blob blob = (Blob) data;
                data = blob.getBytes(1, Long.valueOf(blob.length()).intValue());
            }

            if (data instanceof String) {
                String str = (String) data;
                if (EMPTY_BLOB_FUNCTION.equals(str)) {
                    data = column.isOptional() ? null : "";
                }
            }
            return super.convertBinary(column, fieldDefn, data, mode);
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
        }
    }

    public static Duration stringToDuration(String timeString) {
        Matcher matcher = TIME_FIELD_PATTERN.matcher(timeString);
        if (!matcher.matches()) {
            throw new RuntimeException("Unexpected format for TIME column: " + timeString);
        }

        long hours = Long.parseLong(matcher.group(1));
        long minutes = Long.parseLong(matcher.group(2));
        long seconds = Long.parseLong(matcher.group(3));
        long nanoSeconds = 0;
        String microSecondsString = matcher.group(4);
        if (microSecondsString != null) {
            nanoSeconds = Long.parseLong(Strings.justifyLeft(microSecondsString, 9, '0'));
        }

        if (hours >= 0) {
            return Duration.ofHours(hours)
                    .plusMinutes(minutes)
                    .plusSeconds(seconds)
                    .plusNanos(nanoSeconds);
        } else {
            return Duration.ofHours(hours)
                    .minusMinutes(minutes)
                    .minusSeconds(seconds)
                    .minusNanos(nanoSeconds);
        }
    }
}
