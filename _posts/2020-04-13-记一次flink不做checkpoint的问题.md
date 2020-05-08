---
layout:     post
title:      记一次flink不做checkpoint的问题
subtitle:   记一次flink不做checkpoint的问题
date:       2020-04-13
author:     Sun.Zhu
header-img: img/062.jpg
catalog: true
tags:
    - Flink
    - checkpoint
---

**问题现象：**Flink UI界面查看checkpoint的metrics发现一直没有做checkpoint，仔细排查发现有部分subtask的状态是finished。
下图是测试环境复现问题
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200413234511312.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)
**问题原因：**仔细排查代码后发现source是消费kafka的数据，配置的并行度大于kafka的partition数，导致有部分subtask空闲，然后状态变为finished。后来查看了checkpoint过程的源码得以佐证。
在CheckpointCoordinator类的triggerCheckpoint方法中有如下代码段

```
// check if all tasks that we need to trigger are running.
		// if not, abort the checkpoint
		Execution[] executions = new Execution[tasksToTrigger.length];
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee == null) {
				LOG.info("Checkpoint triggering task {} of job {} is not being executed at the moment. Aborting checkpoint.",
						tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
						job);
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			} else if (ee.getState() == ExecutionState.RUNNING) {
				executions[i] = ee;
			} else {
				LOG.info("Checkpoint triggering task {} of job {} is not in state {} but {} instead. Aborting checkpoint.",
						tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
						job,
						ExecutionState.RUNNING,
						ee.getState());
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
```
ee.getState() == ExecutionState.RUNNING判断execution的状态是否为running，否则不做checkpoint

**问题结论：**在消费kafka的数据时，source的并发度不能超过kafka的partition数，可以小于partition，但是部分subtask就会消费多个partition的数据，导致吞吐达不到最大，理想状态是source并发度等于partition数。
