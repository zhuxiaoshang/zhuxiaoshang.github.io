---
layout:     post
title:      allowedlateness和watermark及窗口触发的条件
subtitle:   allowedlateness和watermark及窗口触发的条件
date:       2019-09-02
author:     Sun.Zhu
header-img: img/190337-1574852617ea0a.jpg
catalog: true
tags:
    - Flink
    - watermark
    - allowedLateness
---

allowedlateness  时间( P)=窗口的endtime+allowedlateness ，作为窗口被释放时间。globle window的默认allowedlateness 为Long.MAX_VALUE,其他窗口默认都是0，所以如果不配置allowedlateness 的话在水印触发了窗口计算后窗口被销毁
假设窗口大小10s，水印允许延迟3s，allowedlateness 允许延迟6s
窗口开始时间 0s
窗口结束时间10s
1.当水印时间13s时触发该窗口的计算，所以正常落在0-10s内的数据可以被窗口计算
2.针对乱序情况，watermark = eventtime-3s，所以在watermark<10s前的乱序数据都可以落到窗口中计算
比如数据eventTime依次为 1、4、8、2、12、6、13，此时2和6是乱序数据，但是都在13之前，所以1、4、5、2、6都可以落在0-10的窗口内在13到达时参与计算
3.如果接入的数据的eventtime<P ，但watermark已经超过窗口的endtime时，会再次触发窗口的计算，每满足一条触发一次（前提是该数据属于这个窗口），如果eventtime超过了最大延迟时间P则丢弃，但是可以通过side output将丢弃的数据输出


测试代码如下

```
Tuple3.of("a", "1", 1551169050000L),
Tuple3.of("aa", "33", 1551169060001L),
Tuple3.of("a", "2", 1551169064000L),
Tuple3.of("a", "3", 1551169059002L),
Tuple3.of("a", "0", 1551169061002L),
Tuple3.of("a", "11", 1551169058002L),
Tuple3.of("b", "5", 15511691067000L),
Tuple3.of("a", "4", 1551169061003L),
Tuple3.of("aa", "44", 1551169079004L),
Tuple3.of("b", "6", 1551169108000L)

public class AllowLatenessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(200);
        final OutputTag<Tuple3<String, String, Long>> outputTag = new OutputTag<Tuple3<String, String, Long>>("side-output") {
        };

        DataStream stream =
                env.addSource(new StreamDataSource()).assignTimestampsAndWatermarks(new PunctuatedWatermarkAssigner()).timeWindowAll(Time.seconds(10))
                        .allowedLateness(Time.milliseconds(5* 1000l)).sideOutputLateData(outputTag)
                        .apply(new AllWindowFunction<Tuple3<String, String, Long>, Object, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Object> out) throws Exception {
                                System.out.println("窗口开始时间："+window.getStart()+",结束时间"+window.getEnd());
                                System.out.println("窗口内数据：" + values.toString());
                            }
                        }).getSideOutput(outputTag);
        stream.print();

        env.execute();

    }
}
```
结果：

```
========currentwatermark:1551169047000
========currentwatermark:1551169057001
========currentwatermark:1551169061000
窗口开始时间：1551169050000,结束时间1551169060000
窗口内数据：[(a,1,1551169050000)]//第一次计算
========currentwatermark:1551169061000
窗口开始时间：1551169050000,结束时间1551169060000
窗口内数据：[(a,1,1551169050000), (a,3,1551169059002)]//第二次计算
========currentwatermark:1551169061000
========currentwatermark:1551169061000
窗口开始时间：1551169050000,结束时间1551169060000
窗口内数据：[(a,1,1551169050000), (a,3,1551169059002), (a,11,1551169058002)]//第三次计算
========currentwatermark:15511691064000
窗口开始时间：1551169060000,结束时间1551169070000
窗口内数据：[(aa,33,1551169060001), (a,2,1551169064000), (a,0,1551169061002)]
========currentwatermark:15511691064000
(a,4,1551169061003)//侧输出被丢弃的数据
========currentwatermark:15511691064000
(aa,44,1551169079004)//侧输出被丢弃的数据
========currentwatermark:15511691064000
(b,6,1551169108000)//侧输出被丢弃的数据
窗口开始时间：15511691060000,结束时间15511691070000
窗口内数据：[(b,5,15511691067000)]
```
