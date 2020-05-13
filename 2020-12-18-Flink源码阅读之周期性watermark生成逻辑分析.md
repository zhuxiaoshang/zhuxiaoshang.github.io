---
layout:     post
title:      Flink源码阅读之周期性watermark生成逻辑分析
subtitle:   Flink源码阅读之周期性watermark生成逻辑分析
date:       2019-12-18
author:     Sun.Zhu
header-img: img/122247-158899816771bb.jpg
catalog: true
tags:
    - Flink
    - watermark
---

在我们的main函数中会设置时间特性和生成水印的时间间隔

```
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
env.getConfig().setAutoWatermarkInterval(200L);
```

不设置的话默认为

```
public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
   this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
   if (characteristic == TimeCharacteristic.ProcessingTime) {
      getConfig().setAutoWatermarkInterval(0);
   } else {
      getConfig().setAutoWatermarkInterval(200);
   }
}
```

即默认ProcessingTime时生成水印的间隔为0
EventTime和IngestionTime默认生成水印间隔为200ms
中间执行过程略。。。
然后看TimestampsAndPeriodicWatermarksOperator类的open方法

```
@Override
public void open() throws Exception {
   super.open();

   currentWatermark = Long.MIN_VALUE;
   watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();

   if (watermarkInterval > 0) {
      long now = getProcessingTimeService().getCurrentProcessingTime();
      getProcessingTimeService().registerTimer(now + watermarkInterval, this);
   }
}
```

从executionConfi中获取watermarkInterval ，大于0时会注册定时器，在now + watermarkInterval后会触发定时器，并将当前对象传递进去作为回调
registerTimer是个抽象方法，有两个实现
![在这里插入图片描述](https://img-blog.csdnimg.cn/2019121817592116.png)
看下这个类SystemProcessingTimeService.java的实现，**SystemProcessingTimeService这个类也是基于processingtime 窗口触发器的定时器实现**，可参考[Flink源码阅读之Window执行过程](https://blog.csdn.net/weixin_41608066/article/details/106058527)

```
public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {

   // delay the firing of the timer by 1 ms to align the semantics with watermark. A watermark
   // T says we won't see elements in the future with a timestamp smaller or equal to T.
   // With processing time, we therefore need to delay firing the timer by one ms.
   long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;

   // we directly try to register the timer and only react to the status on exception
   // that way we save unnecessary volatile accesses for each timer
   try {
      return timerService.schedule(
            new TriggerTask(status, task, checkpointLock, target, timestamp), delay, TimeUnit.MILLISECONDS);
   }
   catch (RejectedExecutionException e) {
      final int status = this.status.get();
      if (status == STATUS_QUIESCED) {
         return new NeverCompleteFuture(delay);
      }
      else if (status == STATUS_SHUTDOWN) {
         throw new IllegalStateException("Timer service is shut down");
      }
      else {
         // something else happened, so propagate the exception
         throw e;
      }
   }
}
```

会起一个TriggerTask线程放入timerService线程池中等待调度。
TriggerTask的run方法

```
public void run() {
   synchronized (lock) {
      try {
         if (serviceStatus.get() == STATUS_ALIVE) {
            target.onProcessingTime(timestamp);
         }
      } catch (Throwable t) {
         TimerException asyncException = new TimerException(t);
         exceptionHandler.handleAsyncException("Caught exception while processing timer.", asyncException);
      }
   }
}
```

这里会回调TimestampsAndPeriodicWatermarksOperator的onProcessingTime方法

```
public void onProcessingTime(long timestamp) throws Exception {
   // register next timer
   Watermark newWatermark = userFunction.getCurrentWatermark();
   if (newWatermark != null && newWatermark.getTimestamp() > currentWatermark) {
      currentWatermark = newWatermark.getTimestamp();
      // emit watermark
      output.emitWatermark(newWatermark);
   }

   long now = getProcessingTimeService().getCurrentProcessingTime();
   getProcessingTimeService().registerTimer(now + watermarkInterval, this);
}
```

这里会先调udf中的getCurrentWatermark获取水印，和当前水印时间判断是否需要发送新的水印，最后再次注册定时器
这样产生了一个循环，就会周期性生成水印了。
