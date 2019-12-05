package com.datapipeline.starter.moka;

import com.datapipeline.clients.connector.schema.base.DpValidationException;
import com.datapipeline.clients.connector.schema.base.SinkColumn;
import com.datapipeline.clients.connector.schema.mysql.type.MySQLSinkSchemaMapper;
import com.datapipeline.clients.connector.schema.mysql.type.MySQLType;

class TextBuilder implements SinkColumn.DefinitionBuilder {

  private static final int TINY_TEXT_MAX_BYTES = 256;
  private static final int TEXT_MAX_BYTES = 65535;
  private static final int MEDIUM_TEXT_MAX_BYTES = 1024 * 1024 * 16 - 1;
  private static final long LONG_TEXT_MAX_BYTES = 1024 * 1024 * 1024 * 1L - 1;

  @Override
  public SinkColumn.Definition build(Integer precision, Integer scale, String schemaName) {
    final String textType;
    if (precision == null || precision < 0 || precision > MEDIUM_TEXT_MAX_BYTES) {
      textType = "LONGTEXT";
      precision = Integer.MAX_VALUE;
    } else if (precision > TEXT_MAX_BYTES) {
      textType = "MEDIUMTEXT";
      precision = MEDIUM_TEXT_MAX_BYTES / MySQLSinkSchemaMapper.MULTI_BYTES_MULTIPLIER;
    } else if (precision > TINY_TEXT_MAX_BYTES) {
      textType = "TEXT";
      precision = TEXT_MAX_BYTES / MySQLSinkSchemaMapper.MULTI_BYTES_MULTIPLIER;
    } else {
      textType = "TINYTEXT";
      precision = TINY_TEXT_MAX_BYTES / MySQLSinkSchemaMapper.MULTI_BYTES_MULTIPLIER;
    }
    return new SinkColumn.Definition(
        precision,
        scale,
        textType,
        MySQLType.TEXT,
        (data, p, s, columnDeclaration) -> {
          final int dataLength = data.toString().getBytes().length;
          if (dataLength > (long) p * MySQLSinkSchemaMapper.MULTI_BYTES_MULTIPLIER) {
            throw DpValidationException.tooLong(columnDeclaration, dataLength);
          }
        });
  }
}
