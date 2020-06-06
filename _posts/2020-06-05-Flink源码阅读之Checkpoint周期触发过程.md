---
layout:     post
title:      Flink源码阅读之Checkpoint周期触发过程
subtitle:   Flink源码阅读之Checkpoint周期触发过程
date:       2020-06-05
author:     Sun.Zhu
header-img: img/122536-1578543936eba9.jpg
catalog: true
tags:
    - Flink
    - Checkpoint
---

Flink的checkpoint原理就不说了，官网以及博客都有说明，有兴趣的同学可以自行查阅。
本文主要从源码层面分析一下checkpoint是如何周期性触发的。
### 分析
首先通过如下配置启用CheckPoint

```java
env.enableCheckpointing(1000);
```

不设置，则默认CheckPoint间隔为-1，即不启用CheckPoint

```java
/** Periodic checkpoint triggering interval. */
private long checkpointInterval = -1; // disabled
```

如不设置则在构建jobGraph时checkpointInterval 会被赋值为Long.MAX_VALUE
StreamingJobGraphGenerator#configureCheckpointing

```java
long interval = cfg.getCheckpointInterval();
if (interval < MINIMAL_CHECKPOINT_TIME) {
	// interval of max value means disable periodic checkpoint
	interval = Long.MAX_VALUE;
}
```
同时会初始化三个列表：

```java
// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());
```
其中， triggerVertices 只包含那些作为 source 的节点，ackVertices 和 commitVertices 均包含所有的节点。

checkpoint的进行是由CheckpointCoordinator发起的，在 ExecutionGraphBuilder#buildGraph 中，如果作业开启了 checkpoint，则会调用 ExecutionGraph.enableCheckpointing() 方法, 这里会创建 CheckpointCoordinator 对象，并注册一个作业状态的监听 CheckpointCoordinatorDeActivator, CheckpointCoordinatorDeActivator 会在作业状态发生改变时得到通知。

```java
ExecuteGraph#enableCheckpointing
checkpointCoordinator = new CheckpointCoordinator(...);

// interval of max long value indicates disable periodic checkpoint,
// the CheckpointActivatorDeactivator should be created only if the interval is not max value
if (interval != Long.MAX_VALUE) {
   // the periodic checkpoint scheduler is activated and deactivated as a result of
   // job status changes (running -> on, all other states -> off)
   registerJobStatusListener(checkpointCoordinator.createActivatorDeactivator());
}
```

当作业状态发送变更时，CheckpointCoordinatorDeActivator 会得到通知并执行notifyJobStatusChange

```java
//ExecuteGraph.java
private void notifyJobStatusChange(JobStatus newState, Throwable error) {
   if (jobStatusListeners.size() > 0) {
      final long timestamp = System.currentTimeMillis();
      final Throwable serializedError = error == null ? null : new SerializedThrowable(error);

      for (JobStatusListener listener : jobStatusListeners) {
         try {
            listener.jobStatusChanges(getJobID(), newState, timestamp, serializedError);
         } catch (Throwable t) {
            LOG.warn("Error while notifying JobStatusListener", t);
         }
      }
   }
}

//CheckpointCoordinatorDeActivator.java
public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
   if (newJobStatus == JobStatus.RUNNING) {
      // start the checkpoint scheduler
      coordinator.startCheckpointScheduler();
   } else {
      // anything else should stop the trigger for now
      coordinator.stopCheckpointScheduler();
   }
}
```

开始触发checkpoint调度

```java
	// --------------------------------------------------------------------------------------------
	//  Periodic scheduling of checkpoints
	// --------------------------------------------------------------------------------------------
public void startCheckpointScheduler() {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			// make sure all prior timers are cancelled
			stopCheckpointScheduler();

			periodicScheduling = true;
			currentPeriodicTrigger = scheduleTriggerWithDelay(getRandomInitDelay());
		}
	}
```

```java
private ScheduledFuture<?> scheduleTriggerWithDelay(long initDelay) {
		return timer.scheduleAtFixedRate(
			new ScheduledTrigger(),
			initDelay, baseInterval, TimeUnit.MILLISECONDS);
	}
```

new ScheduledTrigger()这是调度线程，这里也是用的ScheduledThreadPoolExecutor线程池来调度线程执行，和周期性生成水印调度一样。run方法如下

```java
private final class ScheduledTrigger implements Runnable {

		@Override
		public void run() {
			try {
				triggerCheckpoint(System.currentTimeMillis(), true);
			}
			catch (Exception e) {
				LOG.error("Exception while triggering checkpoint for job {}.", job, e);
			}
		}
	}
```

定时触发checkpoint，具体执行checkpoint过程在

```java
public CheckpointTriggerResult triggerCheckpoint(long timestamp, CheckpointProperties props, @Nullable String externalSavepointLocation, boolean isPeriodic)
```

具体触发checkpoint执行的过程，后面文章再作分析。
### 总结
具体的过程包括以下几点：
1. 通过env配置checkpoint的间隔，即开启checkpoint。
2. 在构建jobgraph时进行checkpoint相关配置。
3. 构建executiongraph时初始化CheckpointCoordinator 对象并注册CheckpointCoordinatorDeActivator监听。
4. 作业状态发生变化时，开启checkpoint调度。


