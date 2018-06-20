# DpStarterKit
DataPipeline DataSystem 第三方开发框架
## API 版本
0.5.9-SNAPSHOT

## 目录
[简介](#简介)  
[系统要求](#系统要求)  
[快速开始](#快速开始)  
[概念](#概念)  
[注意事项](#注意事项)  
[API 介绍](#API-介绍)
- [init](#init)
- [snapshot start](#snapshot-start)
- [snapshot done](#snapshot-done)
- [insert](#insert)
- [delete](#delete)
- [schema change](#schema-change)
- [on stopped](#on-stopped)  

 
## 简介
![](https://github.com/DataPipelineInc/DpStarterKit/blob/master/img/intro2.png)  

Pipe class 项即开发者实现的 `DpSinkPipe` 的完整类名。  

DpStarterKit 是基于 Kafka Connect 框架，封装了 DataPipeline 任务统计调度，任务进度，错误数据处理等与 DataPipeline 产品紧密结合的功能，由开发者通过实现 `DpSinkPipe` 自定义目的地写数据流的行为。目前此套件开放的 API 只针对写入逻辑的实现。


## 系统要求
- 开发： maven，java 1.8
- 操作系统： linux 最小内存 8G
- 其他依赖：docker， docker-compose

## 快速开始
- 下载 repo   
``` git clone https://github.com/DataPipelineInc/DpStarterKit.git```

- 进入根目录， 编译范例 Project  
```mvn clean install -f starter/pom.xml```

- 拷贝 jar 包至 Docker Image 目录  
``` cp starter/target/starter-1.0-SNAPSHOT.jar docker/connect_sink_dp```

- 创建 Docker Image  
``` docker build -t connect_sink_dp docker/connect_sink_dp```

- 修改 docker/docker-compose.yml, 把其中 API_ROOT 修改成运行服务器的网址。

- 启动 DataPipeline 服务  
``` docker-compose -f docker/docker-compose.yml up -d```

- 稍等片刻后浏览器打开部署服务器的网址，进入 DataPipeline 页面。

- 选择建立数据目的地，选择自定义类型，在 Pipe Class 项填写 `com.datapipeline.starter.ConsoleOutputPipe`

- 根据测试需求，建立数据源，选表，建任务，最后激活。

- 查看 Docker container 日志。 
```docker logs -f sinkdp```





## 概念
Kafka Connect 通过开发者实现 `SinkConnector` 以及 `SinkTask` 来消费 Kafka 中的 `ConnectRecord`。在 DataPipeline 的调度机制中，每个任务会建立一个 SourceConnector 和一个 SinkConnector 来对接所有选中的表中的数据。每一个 connector 会启动一个 task。

对于 source， 每个 task 会启动若干线程并行的读取源数据，并且生产包装好的 message 到 Kafka topic, 每一个目的地的表名对应一个 topic。Topic 名的格式为 dptask_[id].[namespace].[dp schema name]。其中 dptask id 为任务 id，namespace 一般为数据源的 database 或者 schema。dp schema name 对应的是选择源目的地对应关系时所填写的名字。
![截图](https://github.com/DataPipelineInc/DpStarterKit/blob/master/img/intro1.jpg)


每个 sink task 会监听所有表所产生的 topics，由固定线程数轮流消费数据。如果 topics 数大于设置的并发数，部分 topics 的消费会进入等待，直到有空闲线程。`DpSinkPipe` 正是这种线程的具体实现。对于开发者来说，我们需要通过实现 `DpSinkPipe` 来定义写数据的逻辑。接下去会介绍各个实现接口的作用以及应用场景。

## 注意事项
1. Kafka Connect 自带 task rebalance 机制，每当有新建任务时，所有 task 都会先停止，然后重启。
2. 不建议在 `DpSinkPipe` 中启动新的线程。每次任务重启的时候，都会新建 Pipe。 如果实在需要，仔细处理线程的终止。保证生命周期与 `DpSinkPipe` 同步。
3. 不要在代码中 catch exception，所有的 exception 都交由上层处理。

## API 介绍

### init
---
```
  /**
   * Do initialization for this Pipe.
   *
   * @param config The config from user input
   */
  public abstract void init(Map<String, String> config);
```

此接口在 `DpSinkPipe` 初始完后调用。一般用于目的地客户端的建立。或者某些全局变量的初始化。它的参数包含了用户在前端创建目的地时候所填写的 json 配置。DataPipeline 将用户填写的 json 转化成 `Map<String, String>` 的形式 传递给此接口。

### schema change
---
```
  /**
   * Loading data requires that the destination schema is the synchronized with the schema of
   * messages, or at least acceptable. This method should make sure that the destination schema can
   * accept the data loading defined in {@link #handleInsert(MemoryBatchMessage, String, boolean)}.
   * Usually it is the place for altering destination table schema when schema has changed.
   *
   * @param lastSchema it will be null upon task restart thus we consider it a schema change event.
   *     When last schema is null, it'd a good practise to fetch destination schema and verify the
   *     current schema is acceptable with the destination schema.
   * @param currSchema the schema of messages currently processing
   * @param dpSchemaName the destination schema name
   * @param primaryKey the primary keys of messages.
   * @param shouldStageData whether the data is first go to stage place
   * @throws Exception any exception during the process. DO NOT catch exception inside this method.
   */
  public abstract void handleSchemaChange(
      ConnectSchema lastSchema,
      ConnectSchema currSchema,
      String dpSchemaName,
      PrimaryKey primaryKey,
      boolean shouldStageData)
      throws Exception;
```
当发现流通的数据中的 schema 产生变化或者任务启动/重启时被调用。应用场景一般为创建目的地表或者修改目的地表结构以保证数据能正确插入目的地表。
`ConnectSchema` 是 Kafka 的 schema 格式，开发者需要处理 Kafka schema 和 目的地的 schema 的 mapping 关系。源端的 column 类型与 Kafka schema 的 mapping 关系查看相应的 https://yiqixie.com/s/home/fcABokWffVTpLbHPTU8KIgvCz
### delete
---
```
  /**
   * @param msg the message that contains the source records that has been deleted.
   * @throws Exception any exception during the process. DO NOT catch exception inside this method.
   */
  public abstract void handleDelete(MemoryBatchMessage msg, String dpSchemaName) throws Exception;
```
删除事件后作为 message 传到这个接口，`MemoryBatchMessage` 可以调用 getDpSinkRecords() 方法获取 被删除的 records map。以 primary key 为这个 map 的 key。

### insert
---
```
  /**
   * @param loadMessage The message contains a map of {@link DpSinkRecord} collection with {@link
   *     PrimaryKey} as key.
   * @param dpSchemaName The destination schema name
   * @param shouldStageData if the message is produced from full select without incremental key. The
   *     source table without incremental key should be full selected and use INSERT-RENAME process
   *     per our design.
   * @throws Exception any exception occurs.
   */
  public abstract void handleInsert(
      MemoryBatchMessage loadMessage, String dpSchemaName, boolean shouldStageData)
      throws Exception;
```
插入和更新 record 会调用此接口。`MemoryBatchMessage` 可以调用 getDpSinkRecords() 方法获取 insert/update 的 records map。以 primary key 为这个 map 的 key。`shouldStageData` 用于判断源表是否是无增量识别字段，是否应该先进入 staging 区域。对于这种无增量字段的源表，DataPipeline 的做法是定时做全量扫描，所以一般做法是将一次全量扫描的数据放到一个 staging 区域， 比如临时表，然后再做替换。

### snapshot start
---
```
  /**
   * This message indicates the start of a full selection of source table only for batch mode with
   * no incremental key.
   *
   * @param primaryKey the primary key of source table.
   * @param dpSchemaName the destination schema name
   */
  public abstract void handleSnapshotStart(
      String dpSchemaName, PrimaryKey primaryKey, ConnectSchema sinkSchema) throws Exception;
```
此事件代表一次无增量识别字段全量扫描的开始。应用场景一般用于 staging 表的创建。

### snapshot done
---
```
  /**
   * This message indicates the end of a full selection of source table.
   *
   * @param primaryKey the primary key of source table.
   * @param dpSchemaName the destination schema name
   */
  public abstract void handleSnapshotDone(String dpSchemaName, PrimaryKey primaryKey)
      throws Exception;
```
此事件代表一次无增识别字段全量扫描的结束。应用场景一般用于重命名 staging 表变成实际目的地的表名。或者类似 rolling log 一样重命名 staging 表 加上序列号， 比如时间。

### context
---
```
  /**
   * Provide a thread-safe cross-pipe object store.
   *
   * @return Map of objects.
   */
  public Map<String, Object> context() {
    return getContext().getGenericStore();
  }
```
这个方法提供了一个 map of object 用于存放跨 `DpSinkPipe` 的变量，比如全局的客户端。


### on stopped
---
```
  /** Called after all Pipes are stopped. */
  public abstract void onStopped();
```
当任务被停止，并且 `DpSinkPipe` 的方法执行返回后，会调用 `onStopped()` 方法。可用于客户端的关闭。这代表这个 Pipe 的生命周期的结束。















