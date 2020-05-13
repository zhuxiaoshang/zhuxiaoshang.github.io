---
layout:     post
title:      Flink多并行度下watermark生成规则
subtitle:   Flink多并行度下watermark生成规则
date:       yyyy-01-01
author:     Sun.Zhu
header-img: img/061.jpg
catalog: true
tags:
    - Flink
    - watermark
---
**一、多并加粗样式行度流的watermarks**

注意：多并行度的情况下，watermark对齐会取所有channel最小的watermark
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190818102113969.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)

**二、单个流多并行度**
测试代码如下：

    public class MutiParaWatermarkTest {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setMaxParallelism(2);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.getConfig().setAutoWatermarkInterval(200l);
            //1565488800000-2019/8/11 10:00:00
            DataStream<String> source = env.fromElements("1,a,1565488800000", "2,a,1565488801000", "3,b,1565488818000", "4,a,1565488810000", "5,b,1565488813000", "6,c,1565488812000", "7,c,1565488816000", "8,d,1565488822000", "9,c,1565488821000", "10,a,1565488831000");
            source.flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
                @Override
                public void flatMap(String value, Collector<Tuple3<String, String, Long>> out) throws Exception {
                    String[] strs = value.split(",");
                    out.collect(Tuple3.of(strs[0], strs[1], Long.parseLong(strs[2])));
                }
            }).setParallelism(2).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {
                Long delay = 3000l;
                Long timestamp=0l;
    
                @Nullable
                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(timestamp - delay);
                }
    
                @Override
                public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                    timestamp = Math.max(timestamp, element.f2);
                    System.out.println("current element is ==" + element.f0 + "," + element.f1 + "," + DateUtilsJDK8.getTime(element.f2) +
                            ", " +
                            "watermark =" + DateUtilsJDK8.getTime(getCurrentWatermark().getTimestamp()));
                    return element.f2;
                }
            }).setParallelism(2).print().setParallelism(2);
            env.execute();
        }
    }

运行结果如下:

    current element is ==1,a,2019-08-11 10:00:00, watermark =2019-08-11 09:59:57
    1> (1,a,1565488800000)
    current element is ==3,b,2019-08-11 10:00:18, watermark =2019-08-11 10:00:15
    1> (3,b,1565488818000)
    current element is ==5,b,2019-08-11 10:00:13, watermark =2019-08-11 10:00:15
    1> (5,b,1565488813000)
    current element is ==7,c,2019-08-11 10:00:16, watermark =2019-08-11 10:00:15
    1> (7,c,1565488816000)
    current element is ==9,c,2019-08-11 10:00:21, watermark =2019-08-11 10:00:18
    1> (9,c,1565488821000)
    
    current element is ==2,a,2019-08-11 10:00:01, watermark =2019-08-11 09:59:58
    2> (2,a,1565488801000)
    current element is ==4,a,2019-08-11 10:00:10, watermark =2019-08-11 10:00:07
    2> (4,a,1565488810000)
    current element is ==6,c,2019-08-11 10:00:12, watermark =2019-08-11 10:00:09
    2> (6,c,1565488812000)
    current element is ==8,d,2019-08-11 10:00:22, watermark =2019-08-11 10:00:19
    2> (8,d,1565488822000)
    current element is ==10,a,2019-08-11 10:00:31, watermark =2019-08-11 10:00:28
    2> (10,a,1565488831000)

以上未按key分组，默认hash分到不同的task中
结论：每个task维护单独的watermark


按key分组后：

    public class MutiParaWatermarkTest {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setMaxParallelism(2);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.getConfig().setAutoWatermarkInterval(200l);
            //1565488800000-2019/8/11 10:00:00
            DataStream<String> source = env.fromElements("1,a,1565488800000", "2,a,1565488801000", "3,b,1565488818000", "4,a,1565488810000", "5,b,1565488813000", "6,c,1565488812000", "7,c,1565488816000", "8,d,1565488822000", "9,c,1565488821000", "10,a,1565488831000");
            source.flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
                @Override
                public void flatMap(String value, Collector<Tuple3<String, String, Long>> out) throws Exception {
                    String[] strs = value.split(",");
                    out.collect(Tuple3.of(strs[0], strs[1], Long.parseLong(strs[2])));
                }
            }).setParallelism(2).keyBy(new KeySelector<Tuple3<String, String, Long>, Object>() {
                @Override
                public Object getKey(Tuple3<String, String, Long> value) throws Exception {
                    return value.f1;
                }
            }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String,
                    String,
                    Long>>() {
                Long delay = 3000l;
                Long timestamp = 0l;
    
                @Nullable
                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(timestamp - delay);
                }
    
                @Override
                public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                    timestamp = Math.max(timestamp, element.f2);
                    System.out.println("current element is ==" + element.f0 + "," + element.f1 + "," + DateUtilsJDK8.getTime(element.f2) +
                            ", " +
                            "watermark =" + DateUtilsJDK8.getTime(getCurrentWatermark().getTimestamp()));
                    return element.f2;
                }
            }).setParallelism(2).print().setParallelism(2);
            env.execute();
        }
    }


    current element is ==6,c,2019-08-11 10:00:12, watermark =2019-08-11 10:00:09
    1> (6,c,1565488812000)
    current element is ==8,d,2019-08-11 10:00:22, watermark =2019-08-11 10:00:19
    1> (8,d,1565488822000)
    current element is ==2,a,2019-08-11 10:00:01, watermark =2019-08-11 09:59:58
    2> (2,a,1565488801000)
    current element is ==4,a,2019-08-11 10:00:10, watermark =2019-08-11 10:00:07
    2> (4,a,1565488810000)
    current element is ==10,a,2019-08-11 10:00:31, watermark =2019-08-11 10:00:28
    2> (10,a,1565488831000)
    current element is ==1,a,2019-08-11 10:00:00, watermark =2019-08-11 10:00:28
    2> (1,a,1565488800000)
    
    current element is ==3,b,2019-08-11 10:00:18, watermark =2019-08-11 10:00:19
    1> (3,b,1565488818000)
    current element is ==5,b,2019-08-11 10:00:13, watermark =2019-08-11 10:00:19
    1> (5,b,1565488813000)
    current element is ==7,c,2019-08-11 10:00:16, watermark =2019-08-11 10:00:19
    1> (7,c,1565488816000)
    current element is ==9,c,2019-08-11 10:00:21, watermark =2019-08-11 10:00:19
    1> (9,c,1565488821000)

相同的key会分配到同一个task上，同一个task的不同key共享同一个watermark
