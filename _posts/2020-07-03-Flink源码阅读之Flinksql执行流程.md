---
layout:     post
title:      Flink源码阅读之Flinksql执行流程
subtitle:   Flink源码阅读之Flinksql执行流程
date:       2020-07-03
author:     Sun.Zhu
header-img: img/231304-1571152384517b.jpg
catalog: true
tags:
    - Flink
    - Flink SQL
---
### 基本结构
Planner接口
负责sql解析、转换成Transformation
Executor接口
负责将planner转换的Transformation生成streamGraph并执行


```java
public interface Planner {

	/**
	 * Retrieves a {@link Parser} that provides methods for parsing a SQL string.
	 *
	 * @return initialized {@link Parser}
	 */
	Parser getParser();

	/**
	 * Converts a relational tree of {@link ModifyOperation}s into a set of runnable
	 * {@link Transformation}s.
	 *
	 * <p>This method accepts a list of {@link ModifyOperation}s to allow reusing common
	 * subtrees of multiple relational queries. Each query's top node should be a {@link ModifyOperation}
	 * in order to pass the expected properties of the output {@link Transformation} such as
	 * output mode (append, retract, upsert) or the expected output type.
	 *
	 * @param modifyOperations list of relational operations to plan, optimize and convert in a
	 * single run.
	 * @return list of corresponding {@link Transformation}s.
	 */
	List<Transformation<?>> translate(List<ModifyOperation> modifyOperations);

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan
	 * to compute the result of the given collection of {@link QueryOperation}s.
	 *
	 * @param operations The collection of relational queries for which the AST
	 * and execution plan will be returned.
	 * @param extended if the plan should contain additional properties such as
	 * e.g. estimated cost, traits
	 */
	String explain(List<Operation> operations, boolean extended);

	/**
	 * Returns completion hints for the given statement at the given cursor position.
	 * The completion happens case insensitively.
	 *
	 * @param statement Partial or slightly incorrect SQL statement
	 * @param position cursor position
	 * @return completion hints that fit at the current cursor position
	 */
	String[] getCompletionHints(String statement, int position);
}
```

### Sql解析

Parser接口 
负责sql解析
有两个实现一个是old planner，另一个是blink planner
flink对sql的解析依赖于calcite

具体实现

```java
@Override
	public List<Operation> parse(String statement) {
		CalciteParser parser = calciteParserSupplier.get();
		FlinkPlannerImpl planner = validatorSupplier.get();
		// parse the sql query
		//依赖calcite将sql语句解析为sqlNode
		SqlNode parsed = parser.parse(statement);

        //将sqlnode转换为Operation
		Operation operation = SqlToOperationConverter.convert(planner, catalogManager, parsed)
			.orElseThrow(() -> new TableException("Unsupported query: " + statement));
		return Collections.singletonList(operation);
	}
	
	/**
	 * This is the main entrance for executing all kinds of DDL/DML {@code SqlNode}s, different
	 * SqlNode will have it's implementation in the #convert(type) method whose 'type' argument
	 * is subclass of {@code SqlNode}.
	 *
	 * @param flinkPlanner FlinkPlannerImpl to convertCreateTable sql node to rel node
	 * @param catalogManager CatalogManager to resolve full path for operations
	 * @param sqlNode SqlNode to execute on
	 */
	public static Optional<Operation> convert(
			FlinkPlannerImpl flinkPlanner,
			CatalogManager catalogManager,
			SqlNode sqlNode) {
		// validate the query
		// 校验sql的合法性
		final SqlNode validated = flinkPlanner.validate(sqlNode);
		SqlToOperationConverter converter = new SqlToOperationConverter(flinkPlanner, catalogManager);
		//对不同的ddl/dml进行转换
		if (validated instanceof SqlCreateTable) {
			return Optional.of(converter.convertCreateTable((SqlCreateTable) validated));
		} else if (validated instanceof SqlDropTable) {
			return Optional.of(converter.convertDropTable((SqlDropTable) validated));
		} else if (validated instanceof SqlAlterTable) {
			return Optional.of(converter.convertAlterTable((SqlAlterTable) validated));
		} else if (validated instanceof SqlCreateFunction) {
			return Optional.of(converter.convertCreateFunction((SqlCreateFunction) validated));
		} else if (validated instanceof SqlAlterFunction) {
			return Optional.of(converter.convertAlterFunction((SqlAlterFunction) validated));
		} else if (validated instanceof SqlDropFunction) {
			return Optional.of(converter.convertDropFunction((SqlDropFunction) validated));
		} else if (validated instanceof RichSqlInsert) {
			return Optional.of(converter.convertSqlInsert((RichSqlInsert) validated));
		} else if (validated instanceof SqlUseCatalog) {
			return Optional.of(converter.convertUseCatalog((SqlUseCatalog) validated));
		} else if (validated instanceof SqlUseDatabase) {
			return Optional.of(converter.convertUseDatabase((SqlUseDatabase) validated));
		} else if (validated instanceof SqlCreateDatabase) {
			return Optional.of(converter.convertCreateDatabase((SqlCreateDatabase) validated));
		} else if (validated instanceof SqlDropDatabase) {
			return Optional.of(converter.convertDropDatabase((SqlDropDatabase) validated));
		} else if (validated instanceof SqlAlterDatabase) {
			return Optional.of(converter.convertAlterDatabase((SqlAlterDatabase) validated));
		} else if (validated instanceof SqlCreateCatalog) {
			return Optional.of(converter.convertCreateCatalog((SqlCreateCatalog) validated));
		} else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
			return Optional.of(converter.convertSqlQuery(validated));
		} else {
			return Optional.empty();
		}
	}
```

举个栗子：

```sql
CREATE TABLE user_behavior (
                    user_id BIGINT,
                    item_id BIGINT,
                    category_id BIGINT,
                    behavior STRING,
                    ts TIMESTAMP(3),
                    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列
                    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列
                ) WITH (
                    'connector.type' = 'kafka',  -- 使用 kafka connector
                    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本
                    'connector.topic' = 'user_behavior',  -- kafka topic
                    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取
                    --'connector.properties.group.id' = '',
                    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址
                    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址
                    'format.type' = 'json'  -- 数据源格式为 json
                )
```

上面的sql经过SqlNode parsed = parser.parse(statement);解析之后如下图：

![\[外链图片转存失败,源站可能有防盗链机制,建议将图片保存下来直接上传(img-GnFJR75e-1593768699547)(/Users/admin/Library/Containers/com.tencent.WeWorkMac/Data/Library/Application Support/WXWork/Temp/ScreenCapture/企业微信截图_13cee561-c9a0-4318-acd6-bd48a418549d.png)\]](https://img-blog.csdnimg.cn/2020070317321386.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)

对应的表名、列名、with属性参数、主键、唯一键、分区键、水印、表注释、表操作（create table、alter table、drop table。。。）都放到SqlNode对象的对应属性中，SqlNode是一个树形结构也就是AST。

看下createTable的处理

```java
    /**
	 * Convert the {@link SqlCreateTable} node.
	 */
	private Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
		// primary key and unique keys are not supported
		if ((sqlCreateTable.getPrimaryKeyList().size() > 0)
			|| (sqlCreateTable.getUniqueKeysList().size() > 0)) {
			throw new SqlConversionException("Primary key and unique key are not supported yet.");
		}

		// set with properties
    //将with参数放到一个map中
		Map<String, String> properties = new HashMap<>();
		sqlCreateTable.getPropertyList().getList().forEach(p ->
			properties.put(((SqlTableOption) p).getKeyString(), ((SqlTableOption) p).getValueString()));
		//schema
		TableSchema tableSchema = createTableSchema(sqlCreateTable);
		String tableComment = sqlCreateTable.getComment().map(comment ->
			comment.getNlsString().getValue()).orElse(null);
		// set partition key
		List<String> partitionKeys = sqlCreateTable.getPartitionKeyList()
			.getList()
			.stream()
			.map(p -> ((SqlIdentifier) p).getSimple())
			.collect(Collectors.toList());

		CatalogTable catalogTable = new CatalogTableImpl(tableSchema,
			partitionKeys,
			properties,
			tableComment);

		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlCreateTable.fullTableName());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		return new CreateTableOperation(
			identifier,
			catalogTable,
			sqlCreateTable.isIfNotExists());
	}
```

对表的DDL操作（比如对table、function、database的操作）会调用到catalogManager对表的DDL进行维护，如果是query语句则会走

```java
else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
			return Optional.of(converter.convertSqlQuery(validated));
		}

private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
		// transform to a relational tree
		RelRoot relational = planner.rel(validated);
		return new PlannerQueryOperation(relational.project());
	}
```

将SqlNode转换为RelNode，

主要包含3个步骤：

1. 推断table类型

2. 推断计算列

3. 推断watermark分配

   ```scala
   val tableSourceTable = new TableSourceTable[T](
       relOptSchema,
       schemaTable.getTableIdentifier,
       erasedRowType,
       statistic,
       tableSource,
       schemaTable.isStreamingMode,
       catalogTable)
   
     // 1. push table scan
   
     // Get row type of physical fields.
     val physicalFields = getRowType
       .getFieldList
       .filter(f => !columnExprs.contains(f.getName))
       .map(f => f.getIndex)
       .toArray
     // Copy this table with physical scan row type.
     val newRelTable = tableSourceTable.copy(tableSource, physicalFields)
     val scan = LogicalTableScan.create(cluster, newRelTable)
     val relBuilder = FlinkRelBuilder.of(cluster, getRelOptSchema)
     relBuilder.push(scan)
   
     val toRexFactory = cluster
         .getPlanner
         .getContext
         .unwrap(classOf[FlinkContext])
         .getSqlExprToRexConverterFactory
   
     // 2. push computed column project
     val fieldNames = erasedRowType.getFieldNames.asScala
     if (columnExprs.nonEmpty) {
       val fieldExprs = fieldNames
           .map { name =>
             if (columnExprs.contains(name)) {
               columnExprs(name)
             } else {
               s"`$name`"
             }
           }.toArray
       val rexNodes = toRexFactory.create(newRelTable.getRowType).convertToRexNodes(fieldExprs)
       relBuilder.projectNamed(rexNodes.toList, fieldNames, true)
     }
   
     // 3. push watermark assigner
     val watermarkSpec = catalogTable
       .getSchema
       // we only support single watermark currently
       .getWatermarkSpecs.asScala.headOption
     if (schemaTable.isStreamingMode && watermarkSpec.nonEmpty) {
       if (TableSourceValidation.hasRowtimeAttribute(tableSource)) {
         throw new TableException(
           "If watermark is specified in DDL, the underlying TableSource of connector" +
               " shouldn't return an non-empty list of RowtimeAttributeDescriptor" +
               " via DefinedRowtimeAttributes interface.")
       }
       val rowtime = watermarkSpec.get.getRowtimeAttribute
       if (rowtime.contains(".")) {
         throw new TableException(
           s"Nested field '$rowtime' as rowtime attribute is not supported right now.")
       }
       val rowtimeIndex = fieldNames.indexOf(rowtime)
       val watermarkRexNode = toRexFactory
           .create(erasedRowType)
           .convertToRexNode(watermarkSpec.get.getWatermarkExpr)
       relBuilder.watermark(rowtimeIndex, watermarkRexNode)
     }
   
     // 4. returns the final RelNode
     relBuilder.build()
   }
   ```

   

对Source table进行推断

见CatalogSourceTable.scala

```scala
  lazy val tableSource: TableSource[T] = findAndCreateTableSource().asInstanceOf[TableSource[T]]
```

TableFactoryUtil会根据DDL中的with参数进行推断

```java
private static <T> TableSource<T> findAndCreateTableSource(Map<String, String> properties) {
   try {
      return TableFactoryService
         .find(TableSourceFactory.class, properties)
         .createTableSource(properties);
   } catch (Throwable t) {
      throw new TableException("findAndCreateTableSource failed.", t);
   }
}
```

主要逻辑在TableFactoryService#filter方法中

```java
private static <T extends TableFactory> List<T> filter(
      List<TableFactory> foundFactories,
      Class<T> factoryClass,
      Map<String, String> properties) {

   Preconditions.checkNotNull(factoryClass);
   Preconditions.checkNotNull(properties);
	 //先根据TableSourceFactory.class 过滤出source的factory
   List<T> classFactories = filterByFactoryClass(
      factoryClass,
      properties,
      foundFactories);
	 //再根据with参数中的connect.type过滤出满足条件的SourceFactory
   List<T> contextFactories = filterByContext(
      factoryClass,
      properties,
      classFactories);

   return filterBySupportedProperties(
      factoryClass,
      properties,
      classFactories,
      contextFactories);
}
```

后续的优化部分其实都是直接基于 RelNode来完成的。



### SQL 转换及优化

转换的流程主要分为四个部分，即 1）将 Operation 转换为 RelNode，2）优化 RelNode，3）转换成 ExecNode，4）转换为底层的 Transformation 算子。

如果是DML操作进过SQL转化后会变为ModifyOperation，就会调用Planner的translate方法。

具体实现在PlannerBase.scala

```scala
override def translate(
    modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
  if (modifyOperations.isEmpty) {
    return List.empty[Transformation[_]]
  }
  // prepare the execEnv before translating
  getExecEnv.configure(
    getTableConfig.getConfiguration,
    Thread.currentThread().getContextClassLoader)
  overrideEnvParallelism()
	//转化为relNode
  val relNodes = modifyOperations.map(translateToRel)
  //SQL优化
  val optimizedRelNodes = optimize(relNodes)
  //转化为execNode
  val execNodes = translateToExecNodePlan(optimizedRelNodes)
  //转化为底层的transfomation
  translateToPlan(execNodes)
}
```

优化比较复杂，Blink主要是基于Calcite自己的优化，并自定义了一些优化逻辑，有兴趣的读者可以自行研究。

### SQL执行

在得到Transformation后的转化逻辑就和streaming模式一致了，可以参考我之前的博客[Flink源码阅读之基于Flink1.10的任务提交流程](https://blog.csdn.net/weixin_41608066/article/details/105559744)和[Flink源码阅读之基于Flink1.10的任务执行流程](
