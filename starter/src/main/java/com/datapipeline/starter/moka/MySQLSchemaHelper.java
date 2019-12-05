package com.datapipeline.starter.moka;

import com.datapipeline.clients.connector.schema.base.SinkColumn;
import com.datapipeline.clients.connector.schema.base.SinkSchemaMapper;
import com.datapipeline.clients.connector.schema.mysql.MySQLVersion;
import com.datapipeline.clients.connector.schema.mysql.type.MySQLSinkSchemaMapper;
import com.datapipeline.clients.connector.schema.mysql.type.MySQLType;
import com.datapipeline.clients.mysql.MySQLConnect;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class MySQLSchemaHelper {

  private final int maxPrefix;
  private final boolean supportFractionalSeconds;

  public MySQLSchemaHelper(String innodbLargePrefix, boolean supportFractionalSeconds) {
    int maxPrefixBytes = "ON".equalsIgnoreCase(innodbLargePrefix) ? 3072 : 767;
    this.maxPrefix = maxPrefixBytes / MySQLSinkSchemaMapper.MULTI_BYTES_MULTIPLIER;
    this.supportFractionalSeconds = supportFractionalSeconds;
  }

  public static MySQLSchemaHelper create(
      MySQLConnect mySQLConnect) throws SQLException {
    String[] innodbLargePrefix = {"OFF"};
    mySQLConnect.query(
        "SHOW VARIABLES LIKE 'innodb_large_prefix'",
        rs -> {
          if (rs.next()) {
            innodbLargePrefix[0] = rs.getString(2);
          }
        });
    if ("ON".equalsIgnoreCase(innodbLargePrefix[0])) {
      mySQLConnect.query(
          "SHOW VARIABLES LIKE 'innodb_default_row_format'",
          rs -> {
            if (rs.next()) {
              String format = rs.getString(2);
              if (!"dynamic".equalsIgnoreCase(format) && !"compressed".equalsIgnoreCase(format)) {
                innodbLargePrefix[0] = "OFF";
              }
            } else {
              innodbLargePrefix[0] = "OFF";
            }
          });
    }

    boolean supportFractionalSeconds = false;
    String[] version = new String[1];
    mySQLConnect.query(
        "SHOW VARIABLES LIKE 'version'",
        rs -> {
          if (rs.next()) {
            version[0] = rs.getString(2);
          }
        });
    String v = version[0];
    if (v != null) {
      // example: 5.5.8-log
      Pattern p = Pattern.compile("^[0-9\\.]*");
      Matcher m = p.matcher(v);
      if (m.find()) {
        if (new MySQLVersion(m.group()).compareTo(new MySQLVersion("5.6.4")) >= 0) {
          // Only start from 5.6.4 does MySQL support TIME/TIMESTAMP/DATETIME with fraction seconds
          // up to 6 digits.
          supportFractionalSeconds = true;
        }
      }
    }
    return new MySQLSchemaHelper(
        innodbLargePrefix[0], supportFractionalSeconds);
  }

  public boolean isSupportFractionalSeconds() {
    return supportFractionalSeconds;
  }

  public int getMaxPrefixLength() {
    return maxPrefix;
  }

  /**
   * MySQL row size has 65,535 bytes size limit. We can replace varchar with TEXT to reduce the size
   * because TEXT and BLOB is use different storage.
   */
  public void adjustSchema(List<SinkColumn> columns) {
    LongAdder totalRowBytes = new LongAdder();
    List<Pair<SinkColumn, Long>> varcharColumns = new ArrayList<>();
    for (SinkColumn column : columns) {
      final int precision = column.getDefinition().getPrecision();
      switch ((MySQLType) column.getDefinition().getType()) {
        case BITS:
          totalRowBytes.add(precision / 8);
          break;
        case BOOLEAN:
          totalRowBytes.add(1);
          break;
        case DATE:
          totalRowBytes.add(4);
          break;
        case DATETIME:
          totalRowBytes.add(8);
          break;
        case DECIMAL:
          totalRowBytes.add(precision + 2);
          break;
        case DOUBLE:
          totalRowBytes.add(8);
          break;
        case FLOAT:
          totalRowBytes.add(4);
          break;
        case BIGINT:
          totalRowBytes.add(8);
          break;
        case SMALLINT:
          totalRowBytes.add(2);
          break;
        case INTEGER:
          totalRowBytes.add(4);
          break;
        case TIME:
          totalRowBytes.add(3);
          break;
        case TIMESTAMP:
          totalRowBytes.add(4);
          break;
        case VARCHAR:
          final long varcharBytes = (long) precision * MySQLSinkSchemaMapper.MULTI_BYTES_MULTIPLIER;
          totalRowBytes.add(varcharBytes);
          varcharColumns.add(new ImmutablePair<>(column, varcharBytes));
          break;
        case YEAR:
          totalRowBytes.add(1);
          break;
        case TEXT:
        case BLOB:
          totalRowBytes.add(12);
          break;
      }
    }
    varcharColumns.sort((o1, o2) -> -o1.getRight().compareTo(o2.getRight()));
    if (totalRowBytes.sum() >= MySQLSinkSchemaMapper.MAX_ROW_BYTES_SIZE) {
      for (Pair<SinkColumn, Long> pair : varcharColumns) {
        if (totalRowBytes.sum() < MySQLSinkSchemaMapper.MAX_ROW_BYTES_SIZE) {
          break;
        }
        SinkColumn column = pair.getLeft();
        SinkColumn adjusted =
            SinkColumn.builder(column.getName())
                .precision(column.getPrecision())
                .create(new TextBuilder());
        columns.set(columns.indexOf(column), adjusted);
        totalRowBytes.add(-pair.getRight());
        totalRowBytes.add(12);
      }
      if (totalRowBytes.sum() >= MySQLSinkSchemaMapper.MAX_ROW_BYTES_SIZE) {
        throw new RuntimeException("Failed to adjust schema to fit 65,535 limit.");
      }
    }
  }
}
