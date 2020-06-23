---
layout:     post
title:      Flink源码阅读之Timer定时器
subtitle:   Flink源码阅读之Timer定时器
date:       2020-06-10
author:     Sun.Zhu
header-img: img/204610-1573821970c02b.jpg
catalog: true
tags:
    - Flink
    - Timer
---

### 概述
在[window执行过程](https://blog.csdn.net/weixin_41608066/article/details/106058527)篇也提到了定时器的注册，在flink中有很多定时器的使用，比如窗口trigger的触发、watermark的周期生成，其实定时器底层是依赖jdk的ScheduledThreadPoolExecutor来调度的。
### 入口
通过TimerService 接口注册和删除定时器。
```java
public interface TimerService {
	long currentProcessingTime();
	long currentWatermark();

	void registerProcessingTimeTimer(long time);
	void registerEventTimeTimer(long time);

	void deleteProcessingTimeTimer(long time);
	void deleteEventTimeTimer(long time);
}
```
Flink 内部使用 InternalTimerService，可以设置 timer 关联的 namespace 和 key。
在 InternalTimeService 中注册的 timer 有两种类型，分别为基于系统时间的和基于事件时间的，它使用两个优先级队列分别保存这两种类型的 timer。Timer 则被抽象为接口 InternalTimer，每个 timer 有绑定的 key，namespace 和触发时间 timestamp，TimerHeapInternalTimer 是其具体实现。InternalTimerServiceImpl 内部的两个优先级队列会按照触发时间的大小进行排序。

Timer 只能在 KeyedStream 中使用，例如在KeyedProcessFunction中注册

```java
ctx.timerService().registerEventTimeTimer(time);
ctx.timerService().registerProcessingTimeTimer(time);
```
### 执行过程
#### Processing time timer

```java
public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
		if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
			// check if we need to re-schedule our timer to earlier
			if (time < nextTriggerTime) {
				if (nextTimer != null) {
					nextTimer.cancel(false);
				}
				nextTimer = processingTimeService.registerTimer(time, this::onProcessingTime);
			}
		}
	}
```
当调用registerProcessingTimeTimer注册后，会尝试将当前time添加到优先级队列中，如果添加失败说明已经存在了同一个key同一个time的定时器已存在。如果当前time时间小于队列头的Timer时间，那么将当前时间注册为下一个待调度的定时器。
具体注册定时器的过程在SystemProcessingTimeService

```java
/**
	 * Registers a task to be executed no sooner than time {@code timestamp}, but without strong
	 * guarantees of order.
	 *
	 * @param timestamp Time when the task is to be enabled (in processing time)
	 * @param callback    The task to be executed
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {

		// delay the firing of the timer by 1 ms to align the semantics with watermark. A watermark
		// T says we won't see elements in the future with a timestamp smaller or equal to T.
		// With processing time, we therefore need to delay firing the timer by one ms.
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.schedule(wrapOnTimerCallback(callback, timestamp), delay, TimeUnit.MILLISECONDS);
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
把callback回调函数封装为ScheduledTask放入线程池中等待调度。
当定时器触发时会回调onProcessingTime方法。触发Triggerable的onProcessingTime方法，执行所有满足条件的定时器，也就是用户实现的udf处理逻辑，并注册下一个定时器。

```java
private void onProcessingTime(long time) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		nextTimer = null;

		InternalTimer<K, N> timer;

		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			processingTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		if (timer != null && nextTimer == null) {
			nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this::onProcessingTime);
		}
	}
```
#### Event time timer
Event time的定时器则依赖于水印的流动。

```java
public void registerEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}
```
注册后会添加到eventTime的优先级队列中，我们知道watermark是一直递增的，随着element在datastream中流动，当watermark被处理时

```java
private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}
```
之后会调用AbstractStreamOperator#processWatermark

```java
public void processWatermark(Watermark mark) throws Exception {
		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}
		output.emitWatermark(mark);
	}
```
最终调用到InternalTimerServiceImpl#advanceWatermark

调用链路是StreamTaskNetworkInput#processElement→StatusWatermarkValve#inputWatermark→StatusWatermarkValve#findAndOutputNewMinWatermarkAcrossAlignedChannels→OneInputStreamTask#emitWatermark→ProcessOperator#processWatermark→AbstractStreamOperator#processWatermark→InternalTimeServiceManager#advanceWatermark→InternalTimerServiceImpl#advanceWatermark


```java
public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		InternalTimer<K, N> timer;

		while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			eventTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}
```
这里就和processing time的类似，从优先级队列中取出满足触发条件的timer，调用udf的onEventTime执行具体逻辑。

### 总结
本文主要分析了定时器Timer在Flink内部的应用，分别分析了基于Processing time和Event time具体的执行逻辑。
