---
layout:     post
title:      两种方式实现Flink异步IO查询Mysql
subtitle:   Flink异步IO查询mysql
date:       2020-05-06
author:     Sun.Zhu
header-img: img/046.jpg
catalog: true
tags:
    - Flink
    - ASync IO
    - mysql
---

如官网所描述的Flink支持两种方式实现异步IO查询外部系统

[https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/asyncio.html](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/asyncio.html)

* 1.数据库(或key/value存储)提供支持异步请求的client。

参考代码：
[https://github.com/zhuxiaoshang/flink-be-god/blob/master/flink-operator/src/main/java/operator/asyncio/ASyncIOClientFunction.java](https://github.com/zhuxiaoshang/flink-be-god/blob/master/flink-operator/src/main/java/operator/asyncio/ASyncIOClientFunction.java)

* 2.没有异步请求客户端的话也可以将同步客户端丢到线程池中执行作为异步客户端。

参考代码：
[https://github.com/zhuxiaoshang/flink-be-god/blob/master/flink-operator/src/main/java/operator/asyncio/ASyncIOFunction.java](https://github.com/zhuxiaoshang/flink-be-god/blob/master/flink-operator/src/main/java/operator/asyncio/ASyncIOFunction.java)

使用io.vertx作为mysql异步调用client

需要添加如下依赖

```java
<dependency>
   <groupId>io.vertx</groupId>
   <artifactId>vertx-jdbc-client</artifactId>
   <version>3.5.4</version>
</dependency>
<dependency>
   <groupId>io.vertx</groupId>
   <artifactId>vertx-core</artifactId>
   <version>3.5.4</version>
</dependency>
```

FLink执行Main函数：
[https://github.com/zhuxiaoshang/flink-be-god/blob/master/flink-operator/src/main/java/operator/asyncio/ASyncIODemo.java](https://github.com/zhuxiaoshang/flink-be-god/blob/master/flink-operator/src/main/java/operator/asyncio/ASyncIODemo.java)




本项目包含常用的flink使用场景，如果对你有帮助，帮忙点个★哦

[https://github.com/zhuxiaoshang/flink-be-god](https://github.com/zhuxiaoshang/flink-be-god)
