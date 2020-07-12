---
layout:     post
title:      Flink实战之自定义flink sql connector
subtitle:   Flink实战之自定义flink sql connector
date:       2020-07-10
author:     Sun.Zhu
header-img: img/9c1db3920437593a7375f89a9cc231bc.jpg
catalog: true
tags:
    - Flink
    - connector
---

## 背景
最近工作中需要自定义开发一些flink sql的connector，因为官方提供的connector毕竟有限，在我们工作中可能会用到各种各样的中间件。所以官方没有提供的就需要我们自定义开发。
就是如：
```sql
CREATE TABLE XXX(
		A STRING,
		B BIGINT)
		WITH(
		'connect.type' = 'kafka',
		...
		)
```
所以开发一个自己的connector需要做哪些，本文就来总结一下开发的主要步骤，以及我遇到的问题怎么解决的。
## 开发
1. 自定义Factory，根据需要实现StreamTableSourceFactory和StreamTableSinkFactory

2. 根据需要继承ConnectorDescriptorValidator，定义自己的connector参数（with 后面跟的那些）

3. Factory中的requiredContext、supportedProperties都比较重要，框架中对Factory的过滤和检查需要他们

4. 需要自定义个TableSink，根据你需要连接的中间件选择是AppendStreamTableSink、Upsert、Retract,并重写consumeDataStream方法

5. 自定义一个SinkFunction，在invoke方法中实现将数据写入到外部中间件。

以上5步基本上可以写一个简单的sql-connector了
## 问题
1. 异常信息：

   ```
   org.apache.flink.table.api.NoMatchingTableFactoryException: Could not find a suitable table factory for 'org.apache.flink.table.factories.TableSinkFactory' in
   the classpath.
   
   Reason: Required context properties mismatch.
   
   The following properties are requested:
   connector.address=localhost:9091
   connector.job=testJob
   connector.metrics=testMetrics
   connector.type=xxxx
   schema.0.data-type=ROW<`val` DOUBLE>
   schema.0.name=value
   ```

   有两个问题都导致上面的报错

   1) discoverFactory时找不到我自定义的Factory

   解决方法：

   添加如下目录及文件

  ![在这里插入图片描述](https://img-blog.csdnimg.cn/20200711004116684.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)

   2) 根据properties过滤时过滤掉了我的Factory，代码在TableFactoryService#filterBySupportedProperties

   ​	解决方法：在自定义Factory#supportedProperties方法中添加schema的配置

   ```
   		// schema
   		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
   		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
   ```

   

   2.序列化问题

   异常信息：

   xxx类不能被序列化

   原因：开始在我的sinkFunction中有个TableSchema属性，该属性不能被序列化，TableSchema我是用来获取字段信息的，后来直接改成了fieldName数组，从TableSchema.getFieldNames()获取。

   改完后又报了我使用的Util类不能序列化，我把util实现了Serializable接口解决

   3.sink类型不匹配问题

   异常如下：

   ```java
   org.apache.flink.table.api.TableException: The StreamTableSink#consumeDataStream(DataStream) must be implemented and return the sink transformation DataStreamSink. However, org.apache.flink.connector.prometheus.xxxTableSink doesn't implement this method.
   
   	at org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSink.translateToPlanInternal(StreamExecSink.scala:142)
   	at org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSink.translateToPlanInternal(StreamExecSink.scala:48)
   	at org.apache.flink.table.planner.plan.nodes.exec.ExecNode$class.translateToPlan(ExecNode.scala:58)
   	at org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSink.translateToPlan(StreamExecSink.scala:48)
   	at org.apache.flink.table.planner.delegation.StreamPlanner$$anonfun$translateToPlan$1.apply(StreamPlanner.scala:60)
   	at org.apache.flink.table.planner.delegation.StreamPlanner$$anonfun$translateToPlan$1.apply(StreamPlanner.scala:59)
   	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
   	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
   	at scala.collection.Iterator$class.foreach(Iterator.scala:891)
   	at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
   	at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
   	at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
   	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
   	at scala.collection.AbstractTraversable.map(Traversable.scala:104)
   	at org.apache.flink.table.planner.delegation.StreamPlanner.translateToPlan(StreamPlanner.scala:59)
   	at org.apache.flink.table.planner.delegation.PlannerBase.translate(PlannerBase.scala:153)
   	at org.apache.flink.table.api.internal.TableEnvironmentImpl.translate(TableEnvironmentImpl.java:682)
   	at org.apache.flink.table.api.internal.TableEnvironmentImpl.sqlUpdate(TableEnvironmentImpl.java:495)
   	at PrometheusConnectorTest.test(PrometheusConnectorTest.java:13)
   	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   	at java.lang.reflect.Method.invoke(Method.java:498)
   	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
   	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
   	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
   	at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
   	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)
   	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)
   	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)
   	at org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)
   	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)
   	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)
   	at org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)
   	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)
   	at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
   	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
   	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:68)
   	at com.intellij.rt.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:33)
   	at com.intellij.rt.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:230)
   	at com.intellij.rt.junit.JUnitStarter.main(JUnitStarter.java:58)
   ```

   但是我xxxTableSink实现的是AppendStreamTableSink，AppendStreamTableSink继承了StreamTableSink

   解决方法：

   实现一个emitDataStream空方法，重写consumeDataStream即可

 > 注意：flink 1.11版本emitDataStream方法已被移除
   ![在这里插入图片描述](https://img-blog.csdnimg.cn/20200713002306967.png)
   
   ```java
   @Override
   	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
   		return dataStream
   			.addSink(new xxxSinkFunction())
   			.setParallelism(dataStream.getParallelism())
   			.name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
   	}
   ```
暂时还没涉及到TableFormat的自定义，如果后面涉及到这方面的开发再来分享
   

## 关于调试
代码开发完后，整个编译一下flink的代码（如果之前编译过，那么只需要编译你的新工程即可）。
有两种方法进行测试
1. 整个编译完后会在flink-dist下生成flink的运行包，并把你的connector包放到lib目录下，通过bin/start-cluster.sh启动单机环境，再通过sql client来测试。如果代码改动后只需要把你的工程重新打包即可。
2. idea里面进行调试（比较推荐这种方式），可以一行行的debug。


详细代码可以参考我的git项目
[https://github.com/zhuxiaoshang/flink-be-god/tree/branch_1.10/flink-connector/flink-sql-connector-customized/src/main/java/sql/connector/customized](https://github.com/zhuxiaoshang/flink-be-god/tree/branch_1.10/flink-connector/flink-sql-connector-customized/src/main/java/sql/connector/customized)

如果对你帮助，帮忙点个star。
