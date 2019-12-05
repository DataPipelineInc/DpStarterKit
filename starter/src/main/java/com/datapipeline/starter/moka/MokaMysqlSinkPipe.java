package com.datapipeline.starter.moka;

import com.datapipeline.base.connector.sink.pipe.message.MemoryBatchMessage;
import com.datapipeline.base.utils.DpTaskRedisUtils;
import com.datapipeline.clients.DpEnv;
import com.datapipeline.clients.JdbcConnect.Config;
import com.datapipeline.clients.JdbcConnectFactory;
import com.datapipeline.clients.TableId;
import com.datapipeline.clients.connector.schema.base.Column;
import com.datapipeline.clients.connector.schema.base.DpColumnRelation;
import com.datapipeline.clients.connector.schema.base.DpSinkRecord;
import com.datapipeline.clients.connector.schema.base.PrimaryKey;
import com.datapipeline.clients.connector.schema.base.RecordConverter;
import com.datapipeline.clients.connector.schema.base.SinkColumn;
import com.datapipeline.clients.connector.schema.base.SinkSchema;
import com.datapipeline.clients.connector.schema.mysql.type.MySQLSinkSchemaMapper;
import com.datapipeline.clients.connector.schema.mysql.type.MySQLType;
import com.datapipeline.clients.mysql.MySQLConnect;
import com.datapipeline.clients.utils.IllegalSchemaNameUtils;
import com.datapipeline.clients.utils.PlaceHolderUtils;
import com.datapipeline.clients.utils.Quoter;
import com.datapipeline.sink.connector.starterkit.DpSinkPipe;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MokaMysqlSinkPipe extends DpSinkPipe {

  private static final Logger log = LoggerFactory.getLogger(MokaMysqlSinkPipe.class);

  private static final Collector<CharSequence, ?, String> COMMA_JOINER = Collectors.joining(", ");
  private static final Collector<CharSequence, ?, String> AND_JOINER =
      Collectors.joining(" AND ");
  private static final String DESTINATION_ENCODING =
      DpEnv.getString(DpEnv.MYSQL_TIDB_DESTINATION_ENCODING, "utf8");
  private static final TableId.QuotedIdentifier QUOTER = Quoter::backQuote;
  private final Boolean enableForeignKeyChecks =
      DpEnv.getBoolean("ENABLE_MYSQL_TIDB_FOREIGN_KEY_CHECKS", true);
  private static final int MAX_TABLE_NAME_LENGTH = 64;
  private static final String STAGE_TABLE_NAME_SUFFIX = "STAGE";
  private boolean skipStageTable;
  private MySQLSinkSchemaMapper mySQLSinkSchemaMapper;
  private StagingTableSwapMode stagingTableSwapMode;
  private String foreignKeySql;
  private String noZeroDateSql;
  private String mb4Sql;


  private MokaPrepareValueSupplier prepareValueSupplier;
  private ImmutableMap<Integer, Config> dataSourceConfigMap;
  private Map<String, String> customConfig;
  private final static String DATA_SOURCE_CONFIG_PATH = "dataDestinationConfigPath";
  private final static String MAX_POOL_SIZE = "maxPoolSize";
  private ImmutableMap<Integer, TableId.Builder> idBuilderMap;
  private SinkSchema sinkSchema;


  @Override
  public void init(Map<String, String> config) {
    this.mySQLSinkSchemaMapper = new MySQLSinkSchemaMapper(true);
    this.skipStageTable = DpEnv.getBoolean(DpEnv.SKIP_STAGE_TABLE, false);
    this.customConfig = config;
    try {
      initDataSourceConfig();
    } catch (IOException e) {
      log.info("Maybe the Database Connect file Not Found", e);
    }
    this.prepareValueSupplier = new MokaPrepareValueSupplier();
    this.stagingTableSwapMode =
        "TRUNCATE".equalsIgnoreCase(DpEnv.getString(DpEnv.STAGING_TABLE_SWAP_MODE))
            ? StagingTableSwapMode.TRUNCATE
            : StagingTableSwapMode.DROP_RENAME;
    if (!DpEnv.getBoolean(DpEnv.ENABLE_ESCAPE_NON_UTF8, true)) {
      this.mb4Sql = "SET NAMES utf8mb4";
    }
    if (!enableForeignKeyChecks) {
      this.foreignKeySql = "SET FOREIGN_KEY_CHECKS=0";
    }
    this.noZeroDateSql = "SET sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'";
  }

  private void initDataSourceConfig() throws IOException {
    final String[] headers = {"hashcode", "hostIp", "user", "password", "port", "database"};
    final List<CSVRecord> csvRecords = FileUtil
        .readCSV(customConfig.get(DATA_SOURCE_CONFIG_PATH), headers);
    Builder<Integer, Config> builder = ImmutableMap.builder();
    Builder<Integer, TableId.Builder> idBuilderMap = ImmutableMap.builder();
    for (CSVRecord csvRecord : csvRecords) {
      Config config = new Config(csvRecord.get("hostIp"), csvRecord.get("port"),
          csvRecord.get("database"),
          csvRecord.get("user"), csvRecord.get("password"),
          Integer.valueOf(customConfig.getOrDefault(MAX_POOL_SIZE, "100")));
      String hashcode = csvRecord.get("hashcode");
      builder.put(Integer.valueOf(hashcode), config);
      GuavaCacheUtil.set(csvRecord.get("hashcode"), JdbcConnectFactory
          .getInstance(MySQLConnect::new, config, null, Integer.valueOf(hashcode)));
      idBuilderMap.put(Integer.valueOf(hashcode), TableId.builder()
          .database(csvRecord.get("database"))
          .tableNameLength(MAX_TABLE_NAME_LENGTH)
          .quotedIdentifier(QUOTER));
    }
    this.idBuilderMap = idBuilderMap.build();
    this.dataSourceConfigMap = builder.build();
  }

  @Override
  public void onStopped() {
    for (Map.Entry<Integer, Config> entry : dataSourceConfigMap.entrySet()) {
      MySQLConnect connect = (MySQLConnect) GuavaCacheUtil.get(String.valueOf(entry.getKey()));
      connect.close();
      GuavaCacheUtil.del(String.valueOf(entry.getKey()));
    }
  }

  @Override
  public void handleSchemaChange(ConnectSchema lastSchema, ConnectSchema currSchema,
      String dpSchemaName, PrimaryKey primaryKey, boolean shouldStageData) throws Exception {
    log.info("Start handle Schema Change module");
    RecordConverter recordConverter = getContext().getRecordConverter();
    updateColumnRelationsByDestColumnName(currSchema, recordConverter);
    sinkSchema = mySQLSinkSchemaMapper.parse(currSchema, recordConverter);
    for (ImmutableMap.Entry<Integer, Config> entry : dataSourceConfigMap.entrySet()) {
      MySQLConnect mysqlConnect = getMysqlConnect(entry.getKey());
      syncSchema(mysqlConnect, destinationTable(entry.getKey(), dpSchemaName), sinkSchema,
          primaryKey);
      if (shouldStageData) {
        syncSchema(mysqlConnect, stageTable(entry.getKey(), dpSchemaName), sinkSchema, primaryKey);
      }
    }
  }

  @Override
  public void handleDelete(MemoryBatchMessage msg, String dpSchemaName) throws Exception {
    for (ImmutableMap.Entry<Integer, Config> entry : dataSourceConfigMap.entrySet()) {
      MySQLConnect mysqlConnect = getMysqlConnect(entry.getKey());
      deleteByPrimaryKey(mysqlConnect, destinationTable(entry.getKey(), dpSchemaName),
          msg.getDpSinkRecords().keySet());
    }
  }

  public void deleteByPrimaryKey(MySQLConnect mySQLConnect, TableId tableId,
      Collection<PrimaryKey> pks) throws Exception {
    if (!pks.isEmpty()) {
      String deleteSql =
          String.format(
              "DELETE FROM %s WHERE (%s) in (%s)",
              tableId.fullName(),
              Iterables.get(pks, 0).getPrimaryKeyNames().stream()
                  .map(Quoter::backQuote)
                  .collect(COMMA_JOINER),
              pks.stream()
                  .map(
                      pk ->
                          String.format(
                              "(%s)",
                              pk.getPrimaryKeys().stream()
                                  .map(
                                      pair ->
                                          Objects.equals(pair.getValue(), null)
                                              ? "null"
                                              : Quoter.singleQuote(pair.getValue().toString()))
                                  .collect(COMMA_JOINER)))
                  .collect(COMMA_JOINER));
      executeWithFKJudge(mySQLConnect, deleteSql);
    }
  }


  class myRunable implements Runnable {

    private MySQLConnect mySQLConnect;
    private Collection<DpSinkRecord> records;
    private SinkSchema sinkSchema;
    private boolean update;
    private String dpSchemaName;

    public myRunable(MySQLConnect mySQLConnect,
        Collection<DpSinkRecord> records,
        SinkSchema sinkSchema, boolean update, String dpSchemaName) {
      this.mySQLConnect = mySQLConnect;
      this.records = records;
      this.sinkSchema = sinkSchema;
      this.update = update;
      this.dpSchemaName = dpSchemaName;
    }

    @Override
    public void run() {
      try {
        handleBatchInsert(mySQLConnect, records, sinkSchema, update,
            dpSchemaName);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void handleInsert(MemoryBatchMessage loadMessage, String dpSchemaName,
      boolean shouldStageData) throws Exception {
    final Collection<DpSinkRecord> records = loadMessage.getDpSinkRecords().values();
    long startTime = System.currentTimeMillis();
    Map<Integer, Collection<DpSinkRecord>> mySQLConnectCollectionMap = distributeConnectPool(
        records);
    long endTime = System.currentTimeMillis();
    log.debug("[MOKA TEST]: records size = {} , deal data time = {} ms", records.size(),
        endTime - startTime);
    long startTime1 = System.currentTimeMillis();
    SinkSchema sinkSchema = mySQLSinkSchemaMapper.parse(
        loadMessage.getBatchMeta().getConnectSchema(), getContext().getRecordConverter());
    mySQLConnectCollectionMap
        .entrySet().parallelStream().map(e -> {
      try {
        TableId targetTable =
            shouldStageData ? stageTable(e.getKey(), dpSchemaName)
                : destinationTable(e.getKey(), dpSchemaName);
        handleBatchInsert(getMysqlConnect(e.getKey()), e.getValue(), sinkSchema,
            false,
            targetTable.fullName());
      } catch (SQLException ex) {
        log.error("Handle Batch Insert error, database = {} , ex = {}",
            dataSourceConfigMap.get(e.getKey()).getDatabase(), ex);
        throw new RuntimeException(
            "Handle Batch Insert error, database = +" + dataSourceConfigMap.get(e.getKey())
                .getDatabase() + ", ex = " + ex);
      }
      return null;
    }).collect(Collectors.toSet());
    long endTime1 = System.currentTimeMillis();
    log.debug("[MOKA TEST]: records size = {} , all insert time = {} ms", records.size(),
        endTime1 - startTime1);
  }

  private void handleBatchInsert(MySQLConnect mySQLConnect,
      Collection<DpSinkRecord> records,
      SinkSchema sinkSchema,
      boolean update, String dpSchemaName) throws SQLException {
    final List<String> columnNames = sinkSchema.getColumnNames();
    final String columnNamesString =
        sinkSchema.getColumnNames().stream().map(Quoter::backQuote).collect(COMMA_JOINER);
    final String placeHoldersString = PlaceHolderUtils.buildSqlPlaceHolders(columnNames.size());
    long startTime = System.currentTimeMillis();
    String sqlFormat = String.format(
        "%s INTO %s (%s) VALUES %s",
        update ? "REPLACE" : "REPLACE",
        dpSchemaName,
        columnNamesString,
        placeHoldersString);
    try {
      mySQLConnect.executePrepareStatement(
          sqlFormat,
          (preparedStatement, connection) ->
              prepareValueSupplier.setValue(
                  connection, preparedStatement, sinkSchema.getColumns(), records, 30, 30),
          Arrays.asList(mb4Sql, foreignKeySql, noZeroDateSql),
          null);
    } catch (SQLException e) {
      throw new SQLException(e);
    }

    long endTime = System.currentTimeMillis();
    log.debug("[MOKA TEST] simple insert data records = {} ,time = {} ms , dest = {}",
        records.size(), endTime - startTime, dpSchemaName);
  }

  @Override
  public void handleSnapshotStart(String dpSchemaName, PrimaryKey primaryKey,
      ConnectSchema connectSchema) throws Exception {
    updateColumnRelationsByDestColumnName(connectSchema, getContext().getRecordConverter());
    final SinkSchema sinkSchema = mySQLSinkSchemaMapper
        .parse(connectSchema, getContext().getRecordConverter());
    for (ImmutableMap.Entry<Integer, Config> entry : dataSourceConfigMap.entrySet()) {
      MySQLConnect mysqlConnect = getMysqlConnect(entry.getKey());
      if (getContext().getDpSinkTaskContext().transactional()) {
        TableId stageTable = stageTable(entry.getKey(), dpSchemaName);
        if (tableExists(mysqlConnect, stageTable)) {
          log.info("Snapshot start - stage table [{}] exists. truncate it.", stageTable);
          truncateTable(mysqlConnect, stageTable);
        } else {
          createTable(mysqlConnect, stageTable, sinkSchema.getColumns(), primaryKey);
          log.info(
              "Snapshot start - stage table [{}] not exists. create it with columns [{}].",
              stageTable,
              sinkSchema.getColumnNames());
        }
        long dataVersion =
            DpTaskRedisUtils.getReadDataVersion(getContext().getDpTaskId(), dpSchemaName);
        if (getContext().getDpSinkTaskContext().getFullDataSyncMode() != null || dataVersion == 1) {
          TableId destTable = destinationTable(entry.getKey(), dpSchemaName);
          if (tableExists(mysqlConnect, destTable)) {
            truncateTable(mysqlConnect, destTable);
            log.info("Snapshot start - dest table [{}] exists. truncate it.", destTable);
          } else {
            createTable(mysqlConnect, destTable, sinkSchema.getColumns(), primaryKey);
            log.info(
                "Snapshot start - dest table [{}] not exists. create it with columns [{}].",
                destTable,
                sinkSchema.getColumnNames());
          }
        }
      } else {
        if (skipStageTable) {
          TableId destinationTable = destinationTable(entry.getKey(), dpSchemaName);
          if (tableExists(mysqlConnect, destinationTable)) {
            truncateTable(mysqlConnect, destinationTable);
          } else {
            createTable(mysqlConnect, destinationTable, sinkSchema.getColumns(), primaryKey);
          }
        } else {
          TableId stageTable = stageTable(entry.getKey(), dpSchemaName);
          if (tableExists(mysqlConnect, stageTable)) {
            log.info("Snapshot start for no incremental key table. {} exists. drop it.",
                stageTable);
            dropTable(mysqlConnect, stageTable);
          } else {
            log.info("Snapshot start for no incremental key table. {} not exists", stageTable);
          }
          log.info("Create table {} with columns {}", stageTable, sinkSchema.getColumnNames());
          createTable(mysqlConnect, stageTable, sinkSchema.getColumns(), primaryKey);
        }
      }
    }
  }

  public void dropTable(MySQLConnect mySQLConnect, TableId id) throws Exception {
    executeWithFKJudge(mySQLConnect, String.format("DROP TABLE %s", id.fullName()));
  }

  @Override
  public void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey) throws Exception {
    ConnectSchema currentConnectSchema = getContext().getCurrentConnectSchema();
    if (getContext().getDpSinkTaskContext().transactional() || !skipStageTable) {
      for (ImmutableMap.Entry<Integer, Config> entry : dataSourceConfigMap.entrySet()) {
        TableId stageTable = stageTable(entry.getKey(), dpSchemaName);
        TableId destTable = destinationTable(entry.getKey(), dpSchemaName);
        MySQLConnect mysqlConnect = getMysqlConnect(entry.getKey());
        log.info("Snapshot done. Check if {} exists.", stageTable.fullName());
        if (tableExists(mysqlConnect, stageTable)) {
          log.info("{} exists.", stageTable);
          // 定时全量
          if (getContext().getDpSinkTaskContext().getFullDataSyncMode() != null) {
            switch (stagingTableSwapMode) {
              case TRUNCATE:
                log.info("Check if {} exists.", destTable);
                if (tableExists(mysqlConnect, destTable)) {
                  log.info("Truncate table : {}", destTable);
                  // truncate destination table
                  truncateTable(mysqlConnect, destTable);
                  log.info("Copy data from {} to {}", stageTable, destTable);
                  copyData(mysqlConnect, stageTable, destTable, currentConnectSchema);
                } else {
                  log.info("Rename {} to {}", stageTable, destTable);
                  renameTable(mysqlConnect, stageTable, destTable);
                }
                break;
              case DROP_RENAME:
                log.info("Check if {} exists.", destTable);
                if (tableExists(mysqlConnect, destTable)) {
                  log.info("Drop destination table: {}", destTable.toString());
                  dropTable(mysqlConnect, destTable);
                }
                log.info("Rename table: {} to {}", stageTable, destTable);
                renameTable(mysqlConnect, stageTable, destTable);
                break;
              default:
            }
          }
          // 定时增量
          else {
            mergeTable(mysqlConnect,
                stageTable,
                destTable,
                sinkSchema.getColumnNames(),
                Optional.ofNullable(primaryKey)
                    .map(PrimaryKey::getPrimaryKeyNames)
                    .orElse(Collections.emptyList()));
          }
        }
      }
    }
  }

  public void mergeTable(MySQLConnect mySQLConnect,
      TableId sourceTable, TableId targetTable, List<String> columnNames, List<String> pkNames)
      throws Exception {
    String columnNameStr = columnNames.stream().map(QUOTER::quote).collect(COMMA_JOINER);
    String keyFilter =
        pkNames.stream()
            .map(column -> String.format("t1.%s=t2.%s", QUOTER.quote(column), QUOTER.quote(column)))
            .collect(AND_JOINER);
    String deleteSql =
        String.format(
            "DELETE t1 FROM %s t1 WHERE EXISTS (SELECT 1 FROM %s t2 where %s)",
            targetTable.fullName(), sourceTable.fullName(), keyFilter);
    String insertSql =
        String.format(
            "INSERT INTO %s(%s) SELECT %s FROM %s ",
            targetTable.fullName(), columnNameStr, columnNameStr, sourceTable.fullName());
    if (enableForeignKeyChecks) {
      mySQLConnect.executeBatch(Arrays.asList(deleteSql, insertSql));
    } else {
      mySQLConnect.executeBatch(Arrays.asList("SET FOREIGN_KEY_CHECKS=0", deleteSql, insertSql));
    }
  }

  public void truncateTable(MySQLConnect mySQLConnect, TableId id) throws Exception {
    mySQLConnect.execute(String.format("TRUNCATE TABLE %s", id.fullName()));
  }

  /**
   * 通过反射进行类型转换
   **/
  private void updateColumnRelationsByDestColumnName(ConnectSchema connectSchema,
      RecordConverter recordConverter)
      throws NoSuchFieldException, IllegalAccessException {

    Class<?> clazz = RecordConverter.class;
    Field field = clazz.getDeclaredField("columnRelationsByDestColumnName");
    field.setAccessible(true);
    Map<String, DpColumnRelation> map = (Map<String, DpColumnRelation>) field.get(recordConverter);
    connectSchema.fields().forEach(s -> {
      String columnName = IllegalSchemaNameUtils.getColumnName(s);
      if (map.get(columnName).getSinkSchemaField() == null) {
        return;
      }
      String schemaFieldType = map.get(columnName).getSinkSchemaField().getSchemaFieldType();
      schemaFieldType = schemaFieldType.replaceAll("\\(.*?\\)", "");
      switch (schemaFieldType) {
        case "tinyint unsigned zerofill":
        case "tinyint unsigned":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("TINYINT_UNSIGNED");
          break;
        case "smallint unsigned zerofill":
        case "smallint unsigned":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("SMALLINT_UNSIGNED");
          break;
        case "mediumint unsigned zerofill":
        case "mediumint unsigned":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("MEDIUMINT_UNSIGNED");
          break;
        case "bigint unsigned zerofill":
        case "bigint unsigned":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("BIGINT_UNSIGNED");
          break;
        case "int unsigned zerofill":
        case "int unsigned":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("INTEGER_UNSIGNED");
          break;
        case "int":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("INTEGER");
          break;
        case "bigint":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("BIGINT");
          break;
        case "mediumint":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("MEDIUMINT");
          break;
        case "smallint":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("SMALLINT");
          break;
        case "blob":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("BLOB");
          break;
        case "date":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("DATE");
          break;
        case "float":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("FLOAT");
          break;
        case "double":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("DOUBLE");
          break;
        case "timestamp":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("TIMESTAMP");
          break;
        case "text":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("TEXT");
          break;
        case "char":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("VARCHAR");
          break;
        case "tinyint":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("TINYINT");
          break;
        case "varchar":
          Optional<Map<String, String>> parameters = Optional
              .of(map.get(columnName).getSourceSchema().parameters());
          Map<String, String> lengthMap = Maps.newHashMap();
          lengthMap.put("length", "255");
          Map<String, String> map1 = parameters.orElse(lengthMap);
          String length = map1.getOrDefault("length", "255");
          map.get(columnName).getSinkSchemaField().setPrecision(Integer.valueOf(length));
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("VARCHAR");
          break;
        case "year":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("YEAR");
          break;
        case "boolean":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("BOOLEAN");
          break;
        case "decimal":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("DECIMAL");
          break;
        case "bit":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("BITS");
          break;
        case "mediumtext":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("MEDIUMTEXT");
          break;
        case "datetime":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("DATETIME");
          break;
        case "varbinary":
          map.get(columnName).getSinkSchemaField().setSchemaFieldType("VARBINARY");
          break;
        default:
      }
    });
    field.set(recordConverter, map);
  }

  public boolean tableExists(MySQLConnect mySQLConnect, TableId targetTable)
      throws Exception {
    boolean[] result = {false};
    mySQLConnect.query(
        String.format(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES"
                + " WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE = 'BASE TABLE' ",
            targetTable.database(), targetTable.table()),
        rs -> {
          if (rs.next()) {
            result[0] = true;
          }
        });
    return result[0];
  }

  private Map<Integer, Collection<DpSinkRecord>> distributeConnectPool(
      Collection<DpSinkRecord> dpSinkRecords)
      throws JSONException {
    Map<Integer, Collection<DpSinkRecord>> dpSinkRecordMap = Maps.newHashMap();
    for (DpSinkRecord dpSinkRecord : dpSinkRecords) {
      JSONObject dataJson = dpSinkRecord.getSchemaJson().getDataJson();
      Object orgId = dataJson.get("org_id");
      if (Objects.isNull(orgId)) {
        throw new RuntimeException("column 'ord_id' is null , dataJson = " + dataJson.toString());
      }
      Integer partitionId = MokaPartitionHelper.INSTANSE.calculateByOrgId(String.valueOf(orgId));
      Collection<DpSinkRecord> dpSinkRecords1 = dpSinkRecordMap.get(partitionId);
      if (dpSinkRecords1 == null) {
        List<DpSinkRecord> list = Lists.newArrayList();
        list.add(dpSinkRecord);
        dpSinkRecordMap.put(partitionId, list);
      } else {
        dpSinkRecords1.add(dpSinkRecord);
        dpSinkRecordMap.put(partitionId, dpSinkRecords1);
      }
    }
    return dpSinkRecordMap;
  }

  public void syncSchema(MySQLConnect mySQLConnect, TableId targetTable, SinkSchema sinkSchema,
      PrimaryKey primaryKey)
      throws Exception {
    log.info("Check if {} exists.", targetTable);
    if (tableExists(mySQLConnect, targetTable)) {
      List<String> targetTableColumnNames =
          mySQLConnect.readColumnsFromDatabase(targetTable).stream()
              .map(Column::getName)
              .collect(Collectors.toList());
      if (!CollectionUtils.isEqualCollection(targetTableColumnNames, sinkSchema.getColumnNames())) {
        log.info(
            "Alter table {}, record columns:{}, table columns:{}",
            targetTable,
            sinkSchema.getColumnNames(),
            targetTableColumnNames);
        alterTable(mySQLConnect, targetTable, sinkSchema, targetTableColumnNames, primaryKey);
      }
    } else {
      log.info("Create table {} with columns:{}", targetTable, sinkSchema.getColumnNames());
      createTable(mySQLConnect, targetTable, sinkSchema.getColumns(), primaryKey);
    }
  }

  private MySQLConnect getMysqlConnect(Integer hashcode) {
    if (hashcode == null) {
      throw new RuntimeException("无法获取mysql连接");
    }
    MySQLConnect mySQLConnect = (MySQLConnect) GuavaCacheUtil.get(String.valueOf(hashcode));
    if (mySQLConnect != null) {
      return mySQLConnect;
    } else {
      Config config = dataSourceConfigMap.get(hashcode);
      return JdbcConnectFactory
          .getInstance(MySQLConnect::new, config, null, hashcode);
    }
  }

  public TableId destinationTable(Integer hashcode, String dpSchemaName) {
    return idBuilderMap.get(hashcode).build(dpSchemaName);
  }

  public TableId stageTable(Integer hashcode, String dpSchemaName) {
    return idBuilderMap.get(hashcode)
        .build(
            dpSchemaName,
            STAGE_TABLE_NAME_SUFFIX,
            getContext().getDestinationSchemaId(getContext().getDpSchemaName()));
  }

  public void createTable(MySQLConnect mySQLConnect, TableId id, List<SinkColumn> columns,
      PrimaryKey primaryKey)
      throws Exception {
    List<SinkColumn> copy = new ArrayList<>(columns);
    MySQLSchemaHelper mySQLSchemaHelper = MySQLSchemaHelper.create(mySQLConnect);
    mySQLSchemaHelper.adjustSchema(copy);
    String format = String.format(
        "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY(%s)) DEFAULT CHARSET=%s",
        id,
        copy.stream()
            .map(
                column ->
                    String.format(
                        "%s %s%s",
                        QUOTER.quote(column.getName()),
                        column.isNotNull()
                            && column.getDefinition().getTypeDeclaration().endsWith(" NULL")
                            ? column
                            .getDefinition()
                            .getTypeDeclaration()
                            .substring(
                                0, column.getDefinition().getTypeDeclaration().length() - 5)
                            : column.getDefinition().getTypeDeclaration(),
                        column.isNotNull() ? " NOT NULL" : ""))
            .collect(COMMA_JOINER),
        copy.stream()
            .filter(col -> primaryKey.getPrimaryKeyNames().contains(col.getName()))
            .map(
                col -> {
                  if (col.getDefinition().getType() == MySQLType.BLOB
                      || col.getDefinition().getType() == MySQLType.TEXT
                      || (col.getDefinition().getType() == MySQLType.VARCHAR
                      && col.getDefinition().getPrecision()
                      > mySQLSchemaHelper.getMaxPrefixLength())) {
                    // Add index prefix length to primary key constraint
                    return String.format(
                        "%s(%s)",
                        QUOTER.quote(col.getName()), mySQLSchemaHelper.getMaxPrefixLength());
                  } else {
                    return QUOTER.quote(col.getName());
                  }
                })
            .collect(COMMA_JOINER),
        DESTINATION_ENCODING);
    if (enableForeignKeyChecks) {
      mySQLConnect.execute(format);
    } else {
      log.info("[moka test] sql = {}", format);
      mySQLConnect.executeBatch(Arrays.asList("SET FOREIGN_KEY_CHECKS=0", format));
    }

  }

  public void copyData(MySQLConnect mySQLConnect, TableId src, TableId dst,
      ConnectSchema connectSchema) throws Exception {
    if (connectSchema == null) {
      // for unit test
      mySQLConnect.execute(
          String.format("INSERT INTO %s SELECT * FROM %s", dst.fullName(), src.fullName()));
    } else {
      List<org.apache.kafka.connect.data.Field> fields = connectSchema.fields();
      List<String> fieldNames =
          fields.stream().map(field -> escapeFieldName(field.name())).collect(Collectors.toList());
      String columns = String.join(",", fieldNames);
      mySQLConnect.execute(
          String.format(
              "INSERT INTO %s(%s) SELECT %s FROM %s",
              dst.fullName(), columns, columns, src.fullName()));
    }
  }

  public void alterTable(
      MySQLConnect mySQLConnect, TableId id, SinkSchema newSchema, List<String> currentColumnNames,
      PrimaryKey primaryKey)
      throws Exception {
    List<String> alters = new ArrayList<>();
    if (!enableForeignKeyChecks) {
      alters.add(foreignKeySql);
    }
    if (currentColumnNames.stream().anyMatch(name -> !newSchema.getColumnNames().contains(name))) {
      alters.add(
          String.format(
              "ALTER TABLE %s %s",
              id.fullName(),
              currentColumnNames.stream()
                  .filter(name -> !newSchema.getColumnNames().contains(name))
                  .map(colName -> "DROP COLUMN " + QUOTER.quote(colName))
                  .collect(COMMA_JOINER)));
    }
    if (newSchema.getColumns().stream()
        .anyMatch(col -> !currentColumnNames.contains(col.getName()))) {
      alters.add(
          String.format(
              "ALTER TABLE %s %s",
              id.fullName(),
              newSchema.getColumns().stream()
                  .filter(col -> !currentColumnNames.contains(col.getName()))
                  .map(
                      col ->
                          String.format(
                              "ADD COLUMN %s %s",
                              QUOTER.quote(col.getName()),
                              col.getDefinition().getTypeDeclaration()))
                  .collect(COMMA_JOINER)));
    }
    mySQLConnect.executeBatch(alters);
  }

  public String escapeFieldName(String fieldName) {
    return Quoter.backQuote(fieldName);
  }

  public void renameTable(MySQLConnect mySQLConnect, TableId old, TableId neo) throws Exception {
    executeWithFKJudge(mySQLConnect,
        String.format("ALTER TABLE %s RENAME %s", old.fullName(), QUOTER.quote(neo.table())));
  }

  private void executeWithFKJudge(MySQLConnect mySQLConnect, String sql) throws Exception {
    if (enableForeignKeyChecks) {
      mySQLConnect.execute(sql);
    } else {
      mySQLConnect.executeBatch(Arrays.asList(foreignKeySql, sql));
    }
  }

  private enum StagingTableSwapMode {
    TRUNCATE,
    DROP_RENAME
  }
}
