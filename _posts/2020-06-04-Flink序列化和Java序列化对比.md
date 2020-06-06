---
layout:     post
title:      Flink序列化和Java序列化对比
subtitle:   Flink序列化和Java序列化对比
date:       2020-06-04
author:     Sun.Zhu
header-img: img/123945-14969831856c4d.jpg
catalog: true
tags:
    - Flink
    - 序列化
---

## Java序列化
Java的序列化机制一般是对象实现Serializable接口，并指定serialVersionUID。通过字节流的方式来实现序列化和反序列化。
serialVersionUID的作用是用来作为版本控制，如果serialVersionUID发生改变则会反序列化失败。
主要用途：
- 用于网络传输
- 对象深拷贝
- 用于将对象存储起来
缺点：
1. 无法跨语言
2. 序列化后码流太大
3. 序列化性能太低

## Flink的序列化
Flink实现了自己的序列化框架，并结合自身的内存模型，实现了对象的密集存储也高效操作。
### Flink序列化框架
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200604194834303.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
- 可以看出这种序列化方式存储密度是相当紧凑的。其中 int 占4字节，double 占8字节，POJO多个一个字节的header，PojoSerializer只负责将header序列化进去，并委托每个字段对应的serializer对字段进行序列化。
- memory pool 内存池 memorySegment的数据结构，由两部分组成，一部分是存储key+pointer（完整二进制数据的指针以及定长的序列化后的key），第二部分是对象的二进制数据
如下图：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200604195139908.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
使用内存池管理内存和使用二进制存储数据的的好处：
1. 避免oom，所有的运行时数据结构和算法只能通过内存池申请内存，保证了其使用的内存大小是固定的，不会因为运行时数据结构和算法而发生OOM。在内存吃紧的情况下，算法（sort/join等）会高效地将一大批内存块写到磁盘，之后再读回来。因此，OutOfMemoryErrors可以有效地被避免。
2. 节省内存空间，Java 对象在存储上有很多额外的消耗，使用二进制可以避免。
3. 高效的二进制操作 & 缓存友好的计算，第一，交换定长块（key+pointer）更高效，不用交换真实的数据也不用移动其他key和pointer。第二，这样做是缓存友好的，因为key都是连续存储在内存中的，可以大大减少 cache miss（cpu读取L1,L2,L3高速缓存速度高于读取主内存速度几个数量级，使用key+pointer极大提高缓存L1,L2,L3命中率）
注意：Flink 中，排序会先用 key 比大小，这样就可以直接用二进制的key比较而不需要反序列化出整个对象。因为key是定长的，如果key相同（或者没有提供二进制key），那就必须将真实的二进制数据反序列化出来，然后再做比较。之后，只需要交换key+pointer就可以达到排序的效果，真实的数据不用移动。
