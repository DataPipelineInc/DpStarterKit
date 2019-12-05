package com.datapipeline.starter.moka;

import com.datapipeline.clients.EscapeUtils;
import com.datapipeline.clients.connector.schema.base.SinkColumn;
import com.datapipeline.clients.connector.schema.mysql.type.MySQLType;
import com.datapipeline.clients.dbz.time.ZonedTimestamp;
import com.datapipeline.sink.consumer.CommonPrepareValueSupplier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


public class MokaPrepareValueSupplier extends CommonPrepareValueSupplier {


  @Override
  public void processNull(PreparedStatement ps, int index, SinkColumn column)
      throws SQLException {
    ps.setObject(index, null);
  }

  @Override
  public void setValue(
      Connection connection,
      PreparedStatement ps,
      SinkColumn column,
      Object value,
      int index,
      int defaultScale,
      int maxScale)
      throws SQLException {
    switch ((MySQLType) column.getDefinition().getType()) {
      case BITS:
      case VARBINARY:
        processBytes(ps, index, value);
        break;
      case BLOB:
        processBytes(ps, index, value);
        break;
      case BOOLEAN:
        processBoolean(ps, index, value);
        break;
      case DATE:
        processDate(ps, index, value);
        break;
      case DATETIME:
      case TIMESTAMP:
        if (column.getSchemaName().equals(ZonedTimestamp.SCHEMA_NAME)) {
          processTimestampTZ(ps, index, value);
        } else {
          processDateTimeString(ps, index, value);
        }
        break;
      case DECIMAL:
        processDecimal(ps, index, value, column, defaultScale, maxScale);
        break;
      case TIME:
        processTime(ps, index, value);
        break;
      case TEXT:
      case DOUBLE:
      case FLOAT:
      case VARCHAR:
      case INTEGER:
      case YEAR:
      default:
        processString(ps, index, value);
        break;
    }
  }

//  protected static void processTimestamp(PreparedStatement ps, int index, Object value)
//      throws SQLException {
//    if (verifyDateFormatter(value.toString(), DATETIME_FORMAT)) {
//      ps.setTimestamp(index,
//          Timestamp.valueOf(handleDatetimeString(value.toString(), DATETIME_FORMAT)));
//    } else if (verifyDateFormatter(value.toString(), DATETIME_FORMAT_ZONE)) {
//      ps.setTimestamp(index,
//          Timestamp.valueOf(handleDatetimeString(value.toString(), DATETIME_FORMAT_ZONE)));
//    }
//  }

  private static final String DATETIME_FORMAT_ZONE = "yyyy-MM-dd'T'HH:mm:ss.SSS Z";
  private static final String DATETIME_FORMAT_UNITY = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS";
  private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static String formatString(String str, int length, String slot) {
    StringBuffer sb = new StringBuffer();
    sb.append(str);

    int count = length - str.length();

    while (count > 0) {
      sb.append(slot);
      count--;
    }

    return sb.toString();
  }

  public static void main(String[] args) {
    String ss = "0000-00-00 00:00:00";
    Timestamp timestamp = Timestamp.valueOf(LocalDateTime.MIN);
    System.out.println(timestamp);
  }

  private static boolean verifyDateFormatter(String strDate, String formatter) {
    if (strDate != null) {
      try {
        SimpleDateFormat df = new SimpleDateFormat(formatter);
        df.parse(strDate);
      } catch (ParseException pe) {
        return false;
      }
    }
    return true;
  }


  private static String handleDatetimeString(String datetimeString, String formatter) {
    String format;
    if (formatter.equals(DATETIME_FORMAT_ZONE)) {
      datetimeString = datetimeString.replace("Z", " UTC");
    }
    SimpleDateFormat sdf1 = new SimpleDateFormat(formatter);
    java.util.Date d;
    try {
      d = sdf1.parse(datetimeString);
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          "Time convert error , formatter must be "
              + formatter
              + "actually it is "
              + datetimeString);
    }
    SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT_UNITY);
    format = sdf.format(d);
    return format;
  }

  @Override
  protected void processString(PreparedStatement ps, int index, Object value) throws SQLException {
    ps.setString(index, EscapeUtils.INSTANCE.escape(value.toString()));
  }

  protected void processDateTimeString(PreparedStatement ps, int index, Object value)
      throws SQLException {
    ps.setString(index, value.toString());
  }


  @Override
  protected void processTime(PreparedStatement ps, int index, Object value) throws SQLException {
    /* use string to avoid fractional time lost. */
    try {
      value = LocalTime.parse(value.toString(), DateTimeFormatter.ISO_OFFSET_TIME);
    } catch (DateTimeParseException ex) {
    }
    ps.setString(index, value.toString());
  }
}
