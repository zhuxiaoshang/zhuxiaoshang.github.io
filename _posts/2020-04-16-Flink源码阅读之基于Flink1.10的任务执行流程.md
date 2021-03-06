---
layout:     post
title:      Flink源码阅读之基于Flink1.10的任务执行流程
subtitle:   Flink源码阅读之基于Flink1.10的任务执行流程
date:       2020-04-16
author:     Sun.Zhu
header-img: img/061.jpg
catalog: true
tags:
    - Flink
    - 执行流程
---

在[基于Flink1.10的任务提交流程](https://blog.csdn.net/weixin_41608066/article/details/105559744)中分析了任务的提交流程。本文基于前文基础上进行job执行流程的源码分析。

前文这里已经已经说明，执行流程就从resetAndStartScheduler（）开始
```java
private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {

		//.......

		startJobMasterServices();
		resetAndStartScheduler();//这里开始正式的任务调度过程了
		//......
	}

```
通过SchedulerNG来调度job的运行
```java
private void resetAndStartScheduler() throws Exception {
//......
final SchedulerNG newScheduler = createScheduler(newJobManagerJobMetricGroup);//通过SchedulerNGFactory来实例化SchedulerNG ，里面包含了ExecutionGraph的生成
//......
schedulerAssignedFuture.thenRun(this::startScheduling);
}
```
SchedulerNGFactory有两个实现：DefaultSchedulerFactory和LegacySchedulerFactory分别创建DefaultScheduler和LegacyScheduler实例，这两者都继承SchedulerBase，实例化时都会调用SchedulerBase的构造方法，其中会构造ExecutionGraph

```java
this.executionGraph = createAndRestoreExecutionGraph(jobManagerJobMetricGroup, checkNotNull(shuffleMaster), checkNotNull(partitionTracker));

private ExecutionGraph createAndRestoreExecutionGraph(
		JobManagerJobMetricGroup currentJobManagerJobMetricGroup,
		ShuffleMaster<?> shuffleMaster,
		JobMasterPartitionTracker partitionTracker) throws Exception {
ExecutionGraph newExecutionGraph = createExecutionGraph(currentJobManagerJobMetricGroup, shuffleMaster, partitionTracker);
//.....
}

private ExecutionGraph createExecutionGraph(
		JobManagerJobMetricGroup currentJobManagerJobMetricGroup,
		ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker) throws JobExecutionException, JobException {

		final FailoverStrategy.Factory failoverStrategy = legacyScheduling ?
			FailoverStrategyLoader.loadFailoverStrategy(jobMasterConfiguration, log) :
			new NoOpFailoverStrategy.Factory();
		//调用ExecutionGraphBuilder.buildGraph来构造ExecutionGraph
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobMasterConfiguration,
			futureExecutor,
			ioExecutor,
			slotProvider,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			restartStrategy,
			currentJobManagerJobMetricGroup,
			blobWriter,
			slotRequestTimeout,
			log,
			shuffleMaster,
			partitionTracker,
			failoverStrategy);
	}
```
然后通过startScheduling进行调度

```java
private void startScheduling() {
		checkState(jobStatusListener == null);
		// register self as job status change listener
		jobStatusListener = new JobManagerJobStatusListener();//注册状态监听器，在触发checkpoint时会用到
		schedulerNG.registerJobStatusListener(jobStatusListener);

		schedulerNG.startScheduling();//开始调度
	}
public final void startScheduling() {
		mainThreadExecutor.assertRunningInMainThread();
		registerJobMetrics();
		startSchedulingInternal();
	}
protected void startSchedulingInternal() {
		final ExecutionGraph executionGraph = getExecutionGraph();
		try {
			executionGraph.scheduleForExecution();//通过executiongraph调度执行
		}
		catch (Throwable t) {
			executionGraph.failGlobal(t);
		}
	}
```

```java
public void scheduleForExecution() throws JobException {

		assertRunningInJobMasterMainThread();

		if (isLegacyScheduling()) {
			LOG.info("Job recovers via failover strategy: {}", failoverStrategy.getStrategyName());
		}

		final long currentGlobalModVersion = globalModVersion;

		if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
			//调度工具类，把流和批调度整合到一起
			final CompletableFuture<Void> newSchedulingFuture = SchedulingUtils.schedule(
				scheduleMode,
				getAllExecutionVertices(),
				this);

			if (state == JobStatus.RUNNING && currentGlobalModVersion == globalModVersion) {
				schedulingFuture = newSchedulingFuture;
				newSchedulingFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

							if (!(strippedThrowable instanceof CancellationException)) {
								// only fail if the scheduling future was not canceled
								failGlobal(strippedThrowable);
							}
						}
					});
			} else {
				newSchedulingFuture.cancel(false);
			}
		}
		else {
			throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
		}
	}
	
public static CompletableFuture<Void> schedule(
			ScheduleMode scheduleMode,
			final Iterable<ExecutionVertex> vertices,
			final ExecutionGraph executionGraph) {

		switch (scheduleMode) {
			case LAZY_FROM_SOURCES:
			case LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST:
				return scheduleLazy(vertices, executionGraph);//批调度

			case EAGER:
				return scheduleEager(vertices, executionGraph);//流调度

			default:
				throw new IllegalStateException(String.format("Schedule mode %s is invalid.", scheduleMode));
		}
	}
```
流程序必须所有提前将所有task都启动成功，批程序不需要提前启动所有task，上游task计算完后可以把结果落盘，下游task在启动运行。

```java
/**
	 * Schedule vertices eagerly. That means all vertices will be scheduled at once.
	 *
	 * @param vertices Topologically sorted vertices to schedule.
	 * @param executionGraph The graph the given vertices belong to.
	 */
	public static CompletableFuture<Void> scheduleEager(
			final Iterable<ExecutionVertex> vertices,
			final ExecutionGraph executionGraph) {
		//......
		// allocate the slots (obtain all their futures)
		//分配slots
		for (ExecutionVertex ev : vertices) {
			// these calls are not blocking, they only return futures
			CompletableFuture<Execution> allocationFuture = ev.getCurrentExecutionAttempt().allocateResourcesForExecution(
				slotProviderStrategy,
				LocationPreferenceConstraint.ALL,
				allPreviousAllocationIds);

			allAllocationFutures.add(allocationFuture);
		}
// this future is complete once all slot futures are complete.
		// the future fails once one slot future fails.
		final ConjunctFuture<Collection<Execution>> allAllocationsFuture = FutureUtils.combineAll(allAllocationFutures);
		//slot都分配成功后
return allAllocationsFuture.thenAccept(
			(Collection<Execution> executionsToDeploy) -> {
				for (Execution execution : executionsToDeploy) {
					try {
						execution.deploy();//执行
					} catch (Throwable t) {
						throw new CompletionException(
							new FlinkException(
								String.format("Could not deploy execution %s.", execution),
								t));
					}
				}
			})		
}
```
把申请的slot通过rpc调用分配给对应的TaskManager执行

```java
/**
	 * Deploys the execution to the previously assigned resource.
	 *
	 * @throws JobException if the execution cannot be deployed to the assigned resource
	 */
	public void deploy() throws JobException {
	//......
	//一些校验
	final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
	//通过RPC调用吧executor提交给taskmanager执行
	CompletableFuture.supplyAsync(() -> taskManagerGateway.submitTask(deployment, rpcTimeout), executor)
				.thenCompose(Function.identity())
				.whenCompleteAsync(
					(ack, failure) -> {
						// only respond to the failure case
						if (failure != null) {
							if (failure instanceof TimeoutException) {
								String taskname = vertex.getTaskNameWithSubtaskIndex() + " (" + attemptId + ')';

								markFailed(new Exception(
									"Cannot deploy task " + taskname + " - TaskManager (" + getAssignedResourceLocation()
										+ ") not responding after a rpcTimeout of " + rpcTimeout, failure));
							} else {
								markFailed(failure);
							}
						}
					},
					jobMasterMainThreadExecutor);
}
```
提交task的过程，最终被提交到TaskExecutor中，创建Task线程并启动。

```java
public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		return taskExecutorGateway.submitTask(tdd, jobMasterId, timeout);
	}
public CompletableFuture<Acknowledge> submitTask(
			TaskDeploymentDescriptor tdd,
			JobMasterId jobMasterId,
			Time timeout) {
		//......校验+一些处理
		Task task = new Task(
				jobInformation,
				taskInformation,
				tdd.getExecutionAttemptId(),
				tdd.getAllocationId(),
				tdd.getSubtaskIndex(),
				tdd.getAttemptNumber(),
				tdd.getProducedPartitions(),
				tdd.getInputGates(),
				tdd.getTargetSlotNumber(),
				memoryManager,
				taskExecutorServices.getIOManager(),
				taskExecutorServices.getShuffleEnvironment(),
				taskExecutorServices.getKvStateService(),
				taskExecutorServices.getBroadcastVariableManager(),
				taskExecutorServices.getTaskEventDispatcher(),
				taskStateManager,
				taskManagerActions,
				inputSplitProvider,
				checkpointResponder,
				aggregateManager,
				blobCacheService,
				libraryCache,
				fileCache,
				taskManagerConfiguration,
				taskMetricGroup,
				resultPartitionConsumableNotifier,
				partitionStateChecker,
				getRpcService().getExecutor());
				
			if (taskAdded) {
				task.startTaskThread();
				//......
				}
}
	public void run() {
		try {
			doRun();
		} finally {
			terminationFuture.complete(executionState);
		}
	}
private void doRun() {
	//......
	//实例化invokable对象
	invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
	invokable.invoke();//执行
	//......
}
```
看看比较常用的StreamTask的构造方法

```java
protected StreamTask(
			Environment environment,
			@Nullable TimerService timerService,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor actionExecutor,
			TaskMailbox mailbox) {

		super(environment);

		this.timerService = timerService;
		this.uncaughtExceptionHandler = Preconditions.checkNotNull(uncaughtExceptionHandler);
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();
		this.recordWriter = createRecordWriterDelegate(configuration, environment);
		this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
		this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor);//调用processInput
		this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);
	}
protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
		InputStatus status = inputProcessor.processInput();
		if (status == InputStatus.MORE_AVAILABLE && recordWriter.isAvailable()) {
			return;
		}
		if (status == InputStatus.END_OF_INPUT) {
			controller.allActionsCompleted();
			return;
		}
		CompletableFuture<?> jointFuture = getInputOutputJointFuture(status);
		MailboxDefaultAction.Suspension suspendedDefaultAction = controller.suspendDefaultAction();
		jointFuture.thenRun(suspendedDefaultAction::resume);
	}
```
processInput处理输入数据，StreamOneInputProcessor#processInput

```java
public InputStatus processInput() throws Exception {
		InputStatus status = input.emitNext(output);

		if (status == InputStatus.END_OF_INPUT) {
			synchronized (lock) {
				operatorChain.endHeadOperatorInput(1);
			}
		}

		return status;
	}

public InputStatus emitNext(DataOutput<T> output) throws Exception {

		while (true) {
			// get the stream element from the deserializer
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);//处理数据
					return InputStatus.MORE_AVAILABLE;
				}
			}

			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
			if (bufferOrEvent.isPresent()) {
				processBufferOrEvent(bufferOrEvent.get());
			} else {
				if (checkpointedInputGate.isFinished()) {
					checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
					if (!checkpointedInputGate.isEmpty()) {
						throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
					}
					return InputStatus.END_OF_INPUT;
				}
				return InputStatus.NOTHING_AVAILABLE;
			}
		}
	}

private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){//是正常数据，走该分支
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {//水印
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}

@Override
		public void emitRecord(StreamRecord<IN> record) throws Exception {
			synchronized (lock) {
				numRecordsIn.inc();
				operator.setKeyContextElement1(record);
				operator.processElement(record);
			}
		}
```
processElement的具体实现就很多了，不同的算子有不同的实现。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200416225814416.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MTYwODA2Ng==,size_16,color_FFFFFF,t_70)

以StreamFlatMap为例，最终会调用udf中的flatMap实现来处理数据。

```java
public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		userFunction.flatMap(element.getValue(), collector);
	}
```
至此job的调用过程基本清晰了。
**总结：**本文分析了flink job，从任务提交到集群后，是如何执行到用户定义的不同算子function实现的过程。
首先jobGraph提交到JM后会生成ExecutionGraph，然后向ResourceManager申请slot资源，申请成功后把slot分配给TaskManager执行具体的task任务。Task会运行不同的算子的实现。

留一个疑问，在Task#doRun方法中会调用invokable.invoke();具体实现有
![在这里插入图片描述](https://img-blog.csdnimg.cn/20200416230733108.png)
以StreamTask为例
```java
public final void invoke() throws Exception {
		try {
			beforeInvoke();//一些初始化操作

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			isRunning = true;
			runMailboxLoop();

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			afterInvoke();
		}
		finally {
			cleanUpInvoke();
		}
	}
private void runMailboxLoop() throws Exception {
		try {
			mailboxProcessor.runMailboxLoop();
		}
		catch (Exception e) {
			Optional<InterruptedException> interruption = ExceptionUtils.findThrowable(e, InterruptedException.class);
			if (interruption.isPresent()) {
				if (!canceled) {
					Thread.currentThread().interrupt();
					throw interruption.get();
				}
			} else if (canceled) {
				LOG.warn("Error while canceling task.", e);
			}
			else {
				throw e;
			}
		}
	}
```

> 问题：
>  MailboxProcessor、TaskMailbox、Mail 这几个概念是什么意思，有什么作用？
> 还有为什么在AbstractInvokable的实例化中处理元素？
> 等我弄明白了再来更新。

4-19来填上面的坑
真正的执行逻辑还是在这一行

```java
// run the invokable
invokable.invoke();
```
然后上面提到在实例化AbstractInvokable时，比如具体的StreamTask,在其构造函数中会把processInput传递给MailboxProcessor的mailboxDefaultAction

```java
protected StreamTask(
			Environment environment,
			@Nullable TimerService timerService,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor actionExecutor,
			TaskMailbox mailbox) {

		super(environment);

		this.timerService = timerService;
		this.uncaughtExceptionHandler = Preconditions.checkNotNull(uncaughtExceptionHandler);
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();
		this.recordWriter = createRecordWriterDelegate(configuration, environment);
		this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
		this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor);
		this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);
	}
```
在StreamTask#invoke()->runMailboxLoop()->mailboxProcessor.runMailboxLoop();中会调用这个mailboxDefaultAction



```java
public void runMailboxLoop() throws Exception {

		final TaskMailbox localMailbox = mailbox;

		Preconditions.checkState(
			localMailbox.isMailboxThread(),
			"Method must be executed by declared mailbox thread!");

		assert localMailbox.getState() == TaskMailbox.State.OPEN : "Mailbox must be opened!";

		final MailboxController defaultActionContext = new MailboxController(this);

		while (processMail(localMailbox)) {
			mailboxDefaultAction.runDefaultAction(defaultActionContext); //就是这里调用defaultAction,也就是processInput方法// lock is acquired inside default action as needed
		}
	}
```
后面的流程就如上面说的一样了

