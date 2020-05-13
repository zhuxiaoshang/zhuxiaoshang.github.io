---
layout:     post
title:      Flink源码阅读之AssignerWithPeriodicWatermarks和AssignerWithPunctuatedWatermarks的区别
subtitle:   AssignerWithPeriodicWatermarks和AssignerWithPunctuatedWatermarks的区别
date:       2019-08-18
author:     Sun.Zhu
header-img: img/121604-1588997764ded0.jpg
catalog: true
tags:
    - Flink
    - watermark
---


[任务提交]()和[任务执行]()分别参考后面文章。

![在这里插入图片描述](https://img-blog.csdnimg.cn/20190828095727580.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
--------------------------------------------------------------------------------
从OneInputStreamTask入口，init（）方法会初始化StreamInputProcessor对象，

    public void init() throws Exception {
       StreamConfig configuration = getConfiguration();
    
       TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
       int numberOfInputs = configuration.getNumberOfInputs();
    
       if (numberOfInputs > 0) {
          InputGate[] inputGates = getEnvironment().getAllInputGates();
    
          inputProcessor = new StreamInputProcessor<>(
                inputGates,
                inSerializer,
                this,
                configuration.getCheckpointMode(),
                getCheckpointLock(),
                getEnvironment().getIOManager(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getStreamStatusMaintainer(),
                this.headOperator,
                getEnvironment().getMetricGroup().getIOMetricGroup(),
                inputWatermarkGauge);
       }
       headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge);
       getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge::getValue);
    }

run方法中会调用StreamInputProcessor的processInput方法，

    protected void run() throws Exception {
       // cache processor reference on the stack, to make the code more JIT friendly
       final StreamInputProcessor<IN> inputProcessor = this.inputProcessor;
    
       while (running && inputProcessor.processInput()) {
          // all the work happens in the "processInput" method
       }
    }


    public boolean processInput() throws Exception {
       if (isFinished) {
          return false;
       }
       if (numRecordsIn == null) {
          try {
             numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
          } catch (Exception e) {
             LOG.warn("An exception occurred during the metrics setup.", e);
             numRecordsIn = new SimpleCounter();
          }
       }
    
       while (true) {
          if (currentRecordDeserializer != null) {
             DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
    
             if (result.isBufferConsumed()) {
                currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
                currentRecordDeserializer = null;
             }
    
             if (result.isFullRecord()) {
                StreamElement recordOrMark = deserializationDelegate.getInstance();
    
                if (recordOrMark.isWatermark()) {
                   // handle watermark 处理watermark
                   statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), currentChannel);
                   continue;
                } else if (recordOrMark.isStreamStatus()) {
                   // handle stream status
                   statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
                   continue;
                } else if (recordOrMark.isLatencyMarker()) {
                   // handle latency marker
                   synchronized (lock) {
                      streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
                   }
                   continue;
                } else {
                   // now we can do the actual processing
                   //  到这开始处理正常数据
                   StreamRecord<IN> record = recordOrMark.asRecord();
                   synchronized (lock) {
                      numRecordsIn.inc();
                      streamOperator.setKeyContextElement1(record);
                      streamOperator.processElement(record);
                   }
                   return true;
                }
             }
          }
    
          final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
          if (bufferOrEvent != null) {
             if (bufferOrEvent.isBuffer()) {
                currentChannel = bufferOrEvent.getChannelIndex();
                currentRecordDeserializer = recordDeserializers[currentChannel];
                currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
             }
             else {
                // Event received
                final AbstractEvent event = bufferOrEvent.getEvent();
                if (event.getClass() != EndOfPartitionEvent.class) {
                   throw new IOException("Unexpected event: " + event);
                }
             }
          }
          else {
             isFinished = true;
             if (!barrierHandler.isEmpty()) {
                throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
             }
             return false;
          }
       }
    }

接下来调用OneInputStreamOperator的processElement方法，实现类如下
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190828095813739.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
常用的是TimestampsAndPunctuatedWatermarksOperator和TimestampsAndPeriodicWatermarksOperator
先看下TimestampsAndPunctuatedWatermarksOperator

    public void processElement(StreamRecord<T> element) throws Exception {
       final T value = element.getValue();
       final long newTimestamp = userFunction.extractTimestamp(value,
             element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);
    
       output.collect(element.replace(element.getValue(), newTimestamp));
    
       final Watermark nextWatermark = userFunction.checkAndGetNextWatermark(value, newTimestamp);
       if (nextWatermark != null && nextWatermark.getTimestamp() > currentWatermark) {
          currentWatermark = nextWatermark.getTimestamp();
          output.emitWatermark(nextWatermark);
       }
    }

先调用用户自定义的extractTimestamp方法获取时间戳，先判断element有没有时间戳，没有则传入Long.MIN_VALUE

    @Override
    public long extractTimestamp(Tuple3<String, String, Long> element, long currentTimestamp) {
        System.out.println("=======WatermarkAssigner:" + currentTimestamp);
        try {
            long etlTime = element.f2;
            currentMaxTimestamp = Math.max(etlTime, currentMaxTimestamp);
            return etlTime;
        } catch (Exception e) {
            LOG.error("the  orderTime parse fail! " + element.toString(), e);
        }
        return 0l;
    }

将元素的timestamp赋值为eventTime，
element.replace(element.getValue(), newTimestamp)

然后调用自定义的checkAndGetNextWatermark获取下一个时间戳，将newTimestamp传入

    @Override
    public Watermark checkAndGetNextWatermark(Tuple3<String, String, Long> lastElement, long watermarkTimestamp) {
        System.out.println("========currentMaxTimestamp:" + currentMaxTimestamp);
        return new Watermark(currentMaxTimestamp - DELAY_TIME);
    }

所以第一个watermark时间就是第一个元素的eventTime-DELAY_TIME，从第二个开始Math.max(eventTime, currentMaxTimestamp)-DELAY_TIME，调用顺序是先extractTimestamp后
checkAndGetNextWatermark。每个元素调用一次。
测试代码

    public class WaterMarkTest {
        public static void main(String[] args) throws Exception{
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.getConfig().setAutoWatermarkInterval(200L);
            env.setParallelism(1);
            env.addSource(new StreamDataSource()).assignTimestampsAndWatermarks(new PunctuatedWatermarkAssigner()).print();
            env.execute();
        }
    }
    
    public class PunctuatedWatermarkAssigner implements AssignerWithPunctuatedWatermarks<Tuple3<String, String, Long>> {
        private static final Logger LOG = LoggerFactory.getLogger(PunctuatedWatermarkAssigner.class);
        private Long currentMaxTimestamp = 0l;
        //水印延迟时间
        private static final Long DELAY_TIME = 3 * 1000l;
    
        /**
         * 再执行该函数，watermarkTimestamp的值是方法extractTimestamp()的返回值
         *
         * @param lastElement        数据流元素
         * @param watermarkTimestamp
         * @return
         */
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<String, String, Long> lastElement, long watermarkTimestamp) {
            System.out.println("========currentwatermark:" + (currentMaxTimestamp-DELAY_TIME));
            return new Watermark(currentMaxTimestamp - DELAY_TIME);
        }
    
        /**
         * 先执行该函数，从element中提取时间戳
         *
         * @param element          数据流元素
         * @param currentTimestamp 当前的系统时间
         * @return 数据的事件时间戳，触发器Trigger中的时间也是这个返回值
         */
        @Override
        public long extractTimestamp(Tuple3<String, String, Long> element, long currentTimestamp) {
            System.out.println("=======WatermarkAssigner:" + currentTimestamp);
            try {
                long etlTime = element.f2;
                currentMaxTimestamp = Math.max(etlTime, currentMaxTimestamp);
                return etlTime;
            } catch (Exception e) {
                LOG.error("the  orderTime parse fail! " + element.toString(), e);
            }
            return 0l;
        }
    }

结果

    =======WatermarkAssigner:-9223372036854775808
    (a,1,1551169050000)
    ========currentwatermark:1551169047000
    =======WatermarkAssigner:-9223372036854775808
    (aa,33,1551169064001)
    ========currentwatermark:1551169061001
    =======WatermarkAssigner:-9223372036854775808
    (a,2,1551169054000)
    ========currentwatermark:1551169061001
    =======WatermarkAssigner:-9223372036854775808
    (a,3,1551169064002)
    ========currentwatermark:1551169061002
    =======WatermarkAssigner:-9223372036854775808
    (b,5,1551169100000)
    ========currentwatermark:1551169097000
    =======WatermarkAssigner:-9223372036854775808
    (a,4,1551169079003)
    ========currentwatermark:1551169097000
    =======WatermarkAssigner:-9223372036854775808
    (aa,44,1551169079004)
    ========currentwatermark:1551169097000
    =======WatermarkAssigner:-9223372036854775808
    (b,6,1551169108000)
    ========currentwatermark:1551169105000

再看下TimestampsAndPeriodicWatermarksOperator

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
       final long newTimestamp = userFunction.extractTimestamp(element.getValue(),
             element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);
    
       output.collect(element.replace(element.getValue(), newTimestamp));
    }

只调用了extractTimestamp，没有调用getCurrentWatermark
同样是先判断当前element有没有timestamp，如果没有则给Long.MIN_VALUE传入extractTimestamp，将返回的eventTime作为时间戳。没有调用getCurrentWatermark方法是因为周期性生成watermark
测试代码

    public class WaterMarkTest {
        public static void main(String[] args) throws Exception{
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.getConfig().setAutoWatermarkInterval(200L);//200ms生成一次watermark
            env.setParallelism(1);
            env.addSource(new StreamDataSource()).assignTimestampsAndWatermarks(new PeriodicWatermarkAssigner()).print();
            env.execute();
        }
    }
    
    public class PeriodicWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>> {
        private static final Logger LOG = LoggerFactory.getLogger(PeriodicWatermarkAssigner.class);
        private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private Long currentMaxTimestamp = 0l;
        //水印延迟时间
        private static final Long DELAY_TIME = 0 * 1000l;
        private Watermark watermark = null;
    
        @Override
        public Watermark getCurrentWatermark() {
            watermark = new Watermark(currentMaxTimestamp - DELAY_TIME);
            System.out.println("currentMaxTimestamp:" + currentMaxTimestamp + " watermark: " + watermark.getTimestamp());
            return watermark;
        }
    
        @Override
        public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
            try {
                System.out.println("WatermarkAssigner:" + l);
                long etlTimestamp = element.f2;
                currentMaxTimestamp = Math.max(etlTimestamp, currentMaxTimestamp);
                return etlTimestamp;
            } catch (Exception e) {
                LOG.error("the orderTime parse fail! " + element.toString(), e);
            }
            return 0l;
        }
    }

结果

    WatermarkAssigner:-9223372036854775808
    (a,1,1551169050000)
    currentMaxTimestamp:1551169050000 watermark: 1551169050000
    currentMaxTimestamp:1551169050000 watermark: 1551169050000
    currentMaxTimestamp:1551169050000 watermark: 1551169050000
    currentMaxTimestamp:1551169050000 watermark: 1551169050000
    WatermarkAssigner:-9223372036854775808
    (aa,33,1551169064001)
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    WatermarkAssigner:-9223372036854775808
    (a,2,1551169054000)
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    currentMaxTimestamp:1551169064001 watermark: 1551169064001
    WatermarkAssigner:-9223372036854775808
    (a,3,1551169064002)
    currentMaxTimestamp:1551169064002 watermark: 1551169064002
    currentMaxTimestamp:1551169064002 watermark: 1551169064002
    currentMaxTimestamp:1551169064002 watermark: 1551169064002
    currentMaxTimestamp:1551169064002 watermark: 1551169064002
    currentMaxTimestamp:1551169064002 watermark: 1551169064002
    WatermarkAssigner:-9223372036854775808
    (b,5,1551169100000)
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    WatermarkAssigner:-9223372036854775808
    (a,4,1551169079003)
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    WatermarkAssigner:-9223372036854775808
    (aa,44,1551169079004)
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    currentMaxTimestamp:1551169100000 watermark: 1551169100000
    WatermarkAssigner:-9223372036854775808
    (b,6,1551169108000)
    currentMaxTimestamp:1551169108000 watermark: 1551169108000
    currentMaxTimestamp:1551169108000 watermark: 1551169108000
    currentMaxTimestamp:1551169108000 watermark: 1551169108000
    currentMaxTimestamp:1551169108000 watermark: 1551169108000
    currentMaxTimestamp:1551169108000 watermark: 1551169108000
    currentMaxTimestamp:1551169108000 watermark: 1551169108000

为何element本来都没有timestamp，都是long的最小值？
那生产实际运行的程序验证一下，结果也是如此

    ====previousElementTimestamp:-9223372036854775808
    orderid =004247789642,orderitemid = 00424778964207,etl_time = 2019-08-27 17:11:48,current wartermark = 2019-08-27 17:11:28
    ====previousElementTimestamp:-9223372036854775808
    orderid =004247sss789642,orderitemid = 00424778964207,etl_time = 2019-08-27 17:11:48,current wartermark = 2019-08-27 17:11:28
    ====previousElementTimestamp:-9223372036854775808
    orderid =004247aaas789642,orderitemid = 00424778964207,etl_time = 2019-08-27 17:11:48,current wartermark = 2019-08-27 17:11:28

那么什么情况下element会有timestamp？
只有在以TimeCharacteristic.IngestionTime进行处理时则element会被打上进入系统的时间

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

设置后结果如下：

    =======WatermarkAssigner:1566899708826
    (a,1,1551169050000)
    ========currentwatermark:1551169047000
    =======WatermarkAssigner:1566899709850
    (aa,33,1551169064001)
    ========currentwatermark:1551169061001
    =======WatermarkAssigner:1566899710851
    (a,2,1551169054000)
    ========currentwatermark:1551169061001
    =======WatermarkAssigner:1566899711851
    (a,3,1551169064002)
    ========currentwatermark:1551169061002
    =======WatermarkAssigner:1566899712851
    (b,5,1551169100000)
    ========currentwatermark:1551169097000
    =======WatermarkAssigner:1566899713855
    (a,4,1551169079003)
    ========currentwatermark:1551169097000
    =======WatermarkAssigner:1566899714855
    (aa,44,1551169079004)
    ========currentwatermark:1551169097000
    =======WatermarkAssigner:1566899715855
    (b,6,1551169108000)
    ========currentwatermark:1551169105000


总结：
AssignerWithPeriodicWatermarks 周期性的生成watermark，生成间隔可配置，根据数据的eventTime来更新watermark时间
AssignerWithPunctuatedWatermarks 不会周期性生成watermark，只根据元素
的eventTime来更新watermark。
当用EventTime和ProcessTime来计算时，元素本身都是不带时间戳的，只有以IngestionTime计算时才会打上进入系统的时间戳。



