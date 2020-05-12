---
layout:     post
title:      Flink CheckPoint详细过程
subtitle:   Flink CheckPoint详细过程
date:       2020-03-24
author:     Sun.Zhu
header-img: img/113958-153525479855be.jpg
catalog: true
tags:
    - Flink
    - CheckPoint
---

Checkpoint由JM的Checkpoint Coordinator发起
 **第一步**，Checkpoint Coordinator 向所有 source 节点 trigger Checkpoint；。

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200324145023248.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
 **第二步**，source 节点向下游广播 barrier，这个 barrier 就是实现 Chandy-Lamport 分布式快照算法的核心，下游的 task 只有收到所有 input 的 barrier 才会执行相应的 Checkpoint。
![在这里插入图片描述](https://img-blog.csdnimg.cn/202003241450376.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)

**第三步**，当 task 完成 state 备份后，会将备份数据的地址（state handle）通知给 Checkpoint coordinator。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200324145045797.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
这里分为同步和异步（如果开启的话）两个阶段：
**1.同步阶段**：task执行状态快照，并写入外部存储系统（根据状态后端的选择不同有所区别）
	执行快照的过程：
	a.对state做深拷贝。
b.将写操作封装在异步的FutureTask中
FutureTask的作用包括：1）打开输入流2）写入状态的元数据信息3）写入状态4）关闭输入流
**2.异步阶段**：
1）执行同步阶段创建的FutureTask
2）向Checkpoint Coordinator发送ACK响应

 **第四步**，下游的 sink 节点收集齐上游两个 input 的 barrier 之后，会执行本地快照，这里特地展示了 RocksDB incremental Checkpoint 的流程，首先 RocksDB 会全量刷数据到磁盘上（红色大三角表示），然后 Flink 框架会从中选择没有上传的文件进行持久化备份（紫色小三角）。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200324145056832.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)

 **同样的**，sink 节点在完成自己的 Checkpoint 之后，会将 state handle 返回通知 Coordinator。

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200324145441914.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
**最后**，当 Checkpoint coordinator 收集齐所有 task 的 state handle，就认为这一次的 Checkpoint 全局完成了，向持久化存储中再备份一个 Checkpoint meta 文件。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200324145456724.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
