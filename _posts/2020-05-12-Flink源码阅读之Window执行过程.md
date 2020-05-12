---
layout:     post
title:      Flink源码阅读之Window执行过程
subtitle:   Flink源码阅读之Window执行过程
date:       2020-05-12
author:     Sun.Zhu
header-img: img/110404-1521083044b19d.jpg
catalog: true
tags:
    - Flink
    - window
---

前面文章介绍了[Flink的任务执行流程](https://blog.csdn.net/weixin_41608066/article/details/105566489)，每一个operator都会有对应的Task去执行，如果程序中使用了window的话，当程序执行到window的task时就会调用WindowOperator中的实现。

```java
	public void processElement(StreamRecord<IN> element) throws Exception {
		//根据元素划分窗口
		final Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;
		//拿到划分的Key
		final K key = this.<K>getKeyedStateBackend().getCurrentKey();
		//如果是session window 则会有合并的过程
		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();
			//...session window这里不讨论
		} else {
			//for循环是因为如果是slide window的话一条数据可能属于多个窗口
			for (W window: elementWindows) {

				// drop if the window is already late
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;
				
				windowState.setCurrentNamespace(window);
				//将数据存入window state
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = window;
				//调用trigger的onElement方法，Trigger可以是eventTime、processTime或自定义的trigger
				TriggerResult triggerResult = triggerContext.onElement(element);
				//如果返回结果是Fire则触发计算
				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(window, contents);
				}
				//如果同时要清除窗口状态的话则清除
				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				//清理定时器
				registerCleanupTimer(window);
			}
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		//迟到的元素通过side output输出
		if (isSkippedElement && isElementLate(element)) {
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}

```
## Trigger
不同的trigger实现有所不同，分别看下eventTime和processTime的
EventTimeTrigger

```java
public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// if the watermark is already past the window fire immediately
			return TriggerResult.FIRE;
		} else {
			ctx.registerEventTimeTimer(window.maxTimestamp());
			return TriggerResult.CONTINUE;
		}
	}
```
判断当前水印是否大于等于窗口结束时间，是的话则返回FIRE，否则将窗口结束时间注册为定时器，返回CONTINUE。

```java
public void registerEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}
```
直接添加到的优先级队列中。

再看ProcessingTimeTrigger

```java
public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
		ctx.registerProcessingTimeTimer(window.maxTimestamp());
		return TriggerResult.CONTINUE;
	}
```
直接注册定时器，返回CONTINUE，因为process time是基于机器系统时间来判断的，跟元素eventtime没有关系。

```java
@Override
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
注册定时器差异比较大，先从优先级队列中取出时间最近的定时器，尝试吧当前时间注册进去，如果成功了，判断当前时间是不是比最近的定时器还要早，是的话就取消最近的定时器。
这里涉及java8的方法引用传递的概念，把onProcessingTime的引用传递给registerTimer
ProcessingTimeServiceImpl
```java
public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		return timerService.registerTimer(timestamp, processingTimeCallbackWrapper.apply(target));
	}
```
SystemProcessingTimeService
```java
/** The executor service that schedules and calls the triggers of this task. */
	private final ScheduledThreadPoolExecutor timerService;
@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {

		// delay the firing of the timer by 1 ms to align the semantics with watermark. A watermark
		// T says we won't see elements in the future with a timestamp smaller or equal to T.
		// With processing time, we therefore need to delay firing the timer by one ms.
		//延迟调度时间=窗口的结束时间-当前时间+1ms
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			//将上面的回调函数包装成task放到线程池中调度
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

	private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long timestamp) {
		//起一个调度线程，将回调函数传递进去
		return new ScheduledTask(status, exceptionHandler, callback, timestamp, 0);
	}
```
当延迟时间到达时会调用ScheduledTask的run方法

```java
public void run() {
			if (serviceStatus.get() != STATUS_ALIVE) {
				return;
			}
			try {
				callback.onProcessingTime(nextTimestamp);
			} catch (Exception ex) {
				exceptionHandler.handleException(ex);
			}
			nextTimestamp += period;
		}
```
上面的callback就是上面onProcessingTime的引用，当触发 InternalTimerServiceImpl.onProcessingTime() 回调后，会从优先级队列中取出所有符合条件的触发器，并调用 triggerTarget.onProcessingTime(timer)。

```java
private void onProcessingTime(long time) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		nextTimer = null;

		InternalTimer<K, N> timer;

		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			processingTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);//调用对应trigger的onProcessingTime方法
		}

		if (timer != null && nextTimer == null) {
		//注册下一个定时器
			nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this::onProcessingTime);
		}
	}
```
这样就会不断循环下去。
上面是Trigger的过程，当窗口触发计算后，如果还需要清除状态，那么就把状态清除，然后取消当前窗口的定时器。

## Evicotr
如果使用了Evictor的话，则会调用EvictingWindowOperator.processElement和上面不同的是emitWindowContents方法

```java
private void emitWindowContents(W window, Iterable<StreamRecord<IN>> contents, ListState<StreamRecord<IN>> windowState) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

		// Work around type system restrictions...
		FluentIterable<TimestampedValue<IN>> recordsWithTimestamp = FluentIterable
			.from(contents)
			.transform(new Function<StreamRecord<IN>, TimestampedValue<IN>>() {
				@Override
				public TimestampedValue<IN> apply(StreamRecord<IN> input) {
					return TimestampedValue.from(input);
				}
			});
		evictorContext.evictBefore(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

		FluentIterable<IN> projectedContents = recordsWithTimestamp
			.transform(new Function<TimestampedValue<IN>, IN>() {
				@Override
				public IN apply(TimestampedValue<IN> input) {
					return input.getValue();
				}
			});

		processContext.window = triggerContext.window;
		userFunction.process(triggerContext.key, triggerContext.window, processContext, projectedContents, timestampedCollector);
		evictorContext.evictAfter(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

		//work around to fix FLINK-4369, remove the evicted elements from the windowState.
		//this is inefficient, but there is no other way to remove elements from ListState, which is an AppendingState.
		windowState.clear();
		for (TimestampedValue<IN> record : recordsWithTimestamp) {
			windowState.add(record.getStreamRecord());
		}
	}
```
在真正窗口处理函数之前和之后分别调用evictBefore和evictAfter方法来做一些过滤。

详细的窗口demo、trigger、evictor、windowFunction的使用样例，参见我的项目
[https://github.com/zhuxiaoshang/flink-be-god](https://github.com/zhuxiaoshang/flink-be-god)
