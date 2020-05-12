---
layout:     post
title:      Kafka Producer序列化问题
subtitle:   Kafka Producer序列化问题
date:       2019-12-10
author:     Sun.Zhu
header-img: img/122003-1588998003e2dc.jpg
catalog: true
tags:
    - Kafka
    - Serialization
---

**报错如下**

```
Caused by: java.lang.ClassCastException: [B cannot be cast to java.lang.String
```
**关键代码：**

```
Properties properties = new Properties();
properties.put("metadata.broker.list", "***");
properties.put("serializer.class","kafka.serializer.StringEncoder");
private Producer<String, byte[]> producer=
        new Producer<String, byte[]>(new kafka.producer.ProducerConfig(properties));
producer.send(new KeyedMessage<String, byte[]>("my_topic",
         SchemaUtils.getSchemaByte(record, schema)));
```
**问题原因：**
kafka producer 序列化有两个配置
serializer.class和key.serializer.class
前者是value，后者是key
且有支持两种配置参数
kafka.serializer.DefaultEncoder 默认的序列化方式为byte数组
kafka.serializer.StringEncoder String序列化

![在这里插入图片描述](https://img-blog.csdnimg.cn/20191210103647831.png)
如上代码 只配置了serializer.class，没有配置key的序列化方式，所以key默认和value的序列化方式一样是String，
但是我们代码发送的是avro格式的byte[]数组，所以报上面的异常
解决**加粗样式**办法：

```
properties.put("key.serializer.class","kafka.serializer.StringEncoder");
```

指定key的序列化方式为String，value的用default方式
