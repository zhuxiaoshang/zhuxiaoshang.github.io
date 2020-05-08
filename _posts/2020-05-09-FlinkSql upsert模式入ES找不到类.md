---
layout:     post
title:      FlinkSql upsert模式入ES找不到类
subtitle:   java.lang.ClassNotFoundException: org.apache.flink.table.typeutils.TypeCheckUtils
date:       2020-05-09
author:     Sun.Zhu
header-img: img/040.jpg
catalog: true
tags:
    - FlinkSql
    - ES
---

**问题现象：** 利用FlinkSql用upsert模式往ES中写数据时报错，append模式却没问题。
报错如下：

```java
Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/flink/table/typeutils/TypeCheckUtils
	at org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.validateKeyTypes(ElasticsearchUpsertTableSinkBase.java:309)
	at org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.setKeyFields(ElasticsearchUpsertTableSinkBase.java:152)
	at org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSink.translateToPlanInternal(StreamExecSink.scala:111)
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
	at org.apache.flink.table.api.internal.TableEnvironmentImpl.insertIntoInternal(TableEnvironmentImpl.java:355)
	at org.apache.flink.table.api.internal.TableEnvironmentImpl.insertInto(TableEnvironmentImpl.java:334)
	at org.apache.flink.table.api.internal.TableImpl.insertInto(TableImpl.java:411)
	at sql.operator.TemproalJoinOperation.main(TemproalJoinOperation.java:44)
Caused by: java.lang.ClassNotFoundException: org.apache.flink.table.typeutils.TypeCheckUtils
	at java.net.URLClassLoader.findClass(URLClassLoader.java:382)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:355)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351)
	... 23 more
```
**问题原因：** 如上异常栈所示，最终会调用到ElasticsearchUpsertTableSinkBase

```java
if (!TypeCheckUtils.isSimpleStringRepresentation(type)) {
                throw new ValidationException("Only simple types that can be safely converted into a string representation can be used as keys. But was: " + type);
            }
```
TypeCheckUtils然而这个类在blink planner包里面有同名的类，但是包名为org.apache.flink.table.runtime.typeutils，而报错显示的包名是org.apache.flink.table.typeutils，所以怀疑是不是少引了包。

后来查证org.apache.flink.table.typeutils.TypeCheckUtils在old planner包中，而我只引入了blink planner的包。

**解决问题：** 引入old planner的包解决问题。

```java
<dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-table-planner_2.11</artifactId>
     <version>${flink-version}</version>
     <scope>provided</scope>
     <exclusions>
     	<exclusion>
          <artifactId>slf4j-api</artifactId>
          <groupId>org.slf4j</groupId>
      	</exclusion>
  	 </exclusions>
</dependency>
```
