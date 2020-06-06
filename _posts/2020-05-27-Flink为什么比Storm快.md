---
layout:     post
title:      Flink为什么比Storm快
subtitle:   Flink为什么比Storm快
date:       2020-05-27
author:     Sun.Zhu
header-img: img/213530-15738249307a8b.jpg
catalog: true
tags:
    - Flink
    - Storm
---

Flink的优势以及具有更多丰富的功能、特性，本文就不提了，网上资料很多。
本文从底层原理分析一下为什么Flink要比Storm快。
“快”说白了就是延迟低。Flink为什么延迟更低主要有以下几个原因：
## 一、数据传输
数据传输有分为进程之间和进程内部。
### 进程之间
进程之间的传输一般包含shuffle的过程，主要是序列化、网络传输、反序列化这三个步骤。
Flink中一般是两个TM之间的传输，通过netty实现。
Storm一般是两个woker间的传输，早期版本通过ZeroMQ实现，后来也改成了netty。
同时Flink有自己的一套[序列化机制](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/types_serialization.html)，并做了很多优化。
### 进程内部
进程内部就是多个线程之间的数据传输。
Flink进程内部，多个逻辑之间可以通过Chain机制，通过一个Task来处理多个算子。通过方法调用传参的形式进程数据传输。
Storm中，两个线程分别运行两个逻辑，通过共享队列进行数据传输。
Flink对于未chain在一起的两个算子，上游算子将计算结果序列化后放入内存，然后通过网络传输给下游算子，下游算子将数据反序列化后继续处理。
对应chain在一起的算子，在一个task内运行，通过对象的**深拷贝**来实现数据传输，如果使用env.getConfig().enableObjectReuse()，Flink会把中间深拷贝的步骤都省略掉，上游算子产生的数据直接作为下游的输入。但需要特别注意的是，这个方法不能随便调用，必须要确保下游Function只有一种，或者下游的Function均不会改变对象内部的值。否则可能会有线程安全的问题。

## 二、可靠性
在Storm中，使用ACK机制来保证数据的可靠性。而在Flink中是通过checkpoint机制来保证的，这是来源于chandy-lamport算法。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200527155849480.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
左边的图展示的是Storm的Ack机制。Spout每发送一条数据到Bolt，就会产生一条ack的信息给acker，当Bolt处理完这条数据后也会发送ack信息给acker。当acker收到这条数据的所有ack信息时，会回复Spout一条ack信息。也就是说，对于一个只有两级（spout+bolt）的拓扑来说，每发送一条数据，就会传输3条ack信息。这3条ack信息则是为了保证可靠性所需要的开销。

右边的图展示的是Flink的Checkpoint机制。Flink中Checkpoint信息的发起者是JobManager。它不像Storm中那样，每条信息都会有ack信息的开销，而且按时间来计算花销。用户可以设置做checkpoint的频率，比如10秒钟做一次checkpoint。每做一次checkpoint，花销只有从Source发往map的1条checkpoint信息（JobManager发出来的checkpoint信息走的是控制流，与数据流无关）。与storm相比，Flink的可靠性机制开销要低得多。这也就是为什么保证可靠性对Flink的性能影响较小，而storm的影响确很大的原因。
## 总结
本文只是对flink和storm框架本身的实现对数据处理延迟的影响，实际场景中肯定会有很多业务的逻辑，这是就会涉及到CPU、内存等资源问题对整体延迟的影响。Flink具有自己的一套[内存管理机制](https://flink.apache.org/news/2015/05/11/Juggling-with-Bits-and-Bytes.html),这也给flink带来性能提升。
