---
layout:     post
title:      Flink源码阅读之Checkpoint执行过程
subtitle:   Flink源码阅读之Checkpoint执行过程
date:       2020-06-18
author:     Sun.Zhu
header-img: img/122536-1578543936eba9.jpg
catalog: true
tags:
    - Flink
    - Checkpoint
---

### 前言
对应Flink来说checkpoint的作用及重要性就不细说了，前面文章写过[checkpoint的详细过程](https://blog.csdn.net/weixin_41608066/article/details/105072042)和[checkpoint周期性触发过程](https://blog.csdn.net/weixin_41608066/article/details/106570023)。不熟悉checkpoint大概过程的同学可以查阅。
本篇我们在一起根据源码看下checkpoint的详细执行过程。
### checkpoint过程
#### 源头
我们都知道checkpoint的周期性触发是由jobmanager中的一个叫做CheckpointCoordinator角色发起的，具体执行在CheckpointCoordinator.triggerCheckpoint中，这个方法代码逻辑很长，概括一下主要包括：
1. 预检查。包括
- 是否需要强制进行 checkpoint
- 当前正在排队的并发 checkpoint 的数目是否超过阈值
- 距离上一次成功 checkpoint 的间隔时间是否过小
如果上述条件不满足则不会进行这次checkpoint。
2. 检查需要触发的task是否都是running状态，否则放弃。之前踩过坑，请见[记一次flink不做checkpoint的问题](https://blog.csdn.net/weixin_41608066/article/details/105500929)。
3. 检查所有需要ack checkpoint完成的task是否都是running状态。否则放弃。
上面的检查都通过之后就可以做checkpoint啦。
4. 生成唯一自增的checkpointID。
5. 初始化CheckpointStorageLocation，用于存储这次checkpoint快照的路径，不同的backend有区别。
6. 生成 PendingCheckpoint，这表示一个处于中间状态的 checkpoint，并保存在 checkpointId -> PendingCheckpoint 这样的映射关系中。
7. 注册一个调度任务，在 checkpoint 超时后取消此次 checkpoint，并重新触发一次新的 checkpoint
8. 调用 Execution.triggerCheckpoint() 方法向所有需要 trigger 的 task 发起 checkpoint 请求

```java
for (Execution execution: executions) {
				if (props.isSynchronous()) {
					execution.triggerSynchronousSavepoint(checkpointID, timestamp, checkpointOptions, advanceToEndOfTime);
				} else {
					execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);
				}
			}
```
最终通过 RPC 调用 TaskExecutorGateway.triggerCheckpoint，即请求执行 TaskExecutor.triggerCheckpoin()。 因为一个 TaskExecutor 中可能有多个 Task 正在运行，因而要根据触发 checkpoint 的 ExecutionAttemptID 找到对应的 Task，然后调用 Task.triggerCheckpointBarrier() 方法
```java
private void triggerCheckpointHelper(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {

		final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
		if (advanceToEndOfEventTime && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
			throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
		}

		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions, advanceToEndOfEventTime);
		} else {
			LOG.debug("The execution has no slot assigned. This indicates that the execution is no longer running.");
		}
	}
```


```java
@Override
	public CompletableFuture<Acknowledge> triggerCheckpoint(
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			long checkpointTimestamp,
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime) {
		log.debug("Trigger checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

		final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
		if (advanceToEndOfEventTime && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
			throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
		}

		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions, advanceToEndOfEventTime);

			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			final String message = "TaskManager received a checkpoint request for unknown task " + executionAttemptID + '.';

			log.debug(message);
			return FutureUtils.completedExceptionally(new CheckpointException(message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
		}
	}
```
Task 执行 checkpoint 的真正逻辑被封装在 AbstractInvokable.triggerCheckpointAsync(...) 中，

```java
public void triggerCheckpointBarrier(
			final long checkpointID,
			final long checkpointTimestamp,
			final CheckpointOptions checkpointOptions,
			final boolean advanceToEndOfEventTime) {

		final AbstractInvokable invokable = this.invokable;
		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointID, checkpointTimestamp);

		if (executionState == ExecutionState.RUNNING && invokable != null) {
			try {
				invokable.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
			}
			catch (RejectedExecutionException ex) {
				// This may happen if the mailbox is closed. It means that the task is shutting down, so we just ignore it.
				LOG.debug(
					"Triggering checkpoint {} for {} ({}) was rejected by the mailbox",
					checkpointID, taskNameWithSubtask, executionId);
			}
			catch (Throwable t) {
				if (getExecutionState() == ExecutionState.RUNNING) {
					failExternally(new Exception(
						"Error while triggering checkpoint " + checkpointID + " for " +
							taskNameWithSubtask, t));
				} else {
					LOG.debug("Encountered error while triggering checkpoint {} for " +
						"{} ({}) while being not in state running.", checkpointID,
						taskNameWithSubtask, executionId, t);
				}
			}
		}
		else {
			LOG.debug("Declining checkpoint request for non-running task {} ({}).", taskNameWithSubtask, executionId);

			// send back a message that we did not do the checkpoint
			checkpointResponder.declineCheckpoint(jobId, executionId, checkpointID,
					new CheckpointException("Task name with subtask : " + taskNameWithSubtask, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
		}
	}
```
triggerCheckpointAsync方法分别被SourceStreamTask和普通StreamTask覆盖，主要逻辑还是在StreamTask中

```java
private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics,
			boolean advanceToEndOfTime) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		final long checkpointId = checkpointMetaData.getCheckpointId();

		if (isRunning) {
			actionExecutor.runThrowing(() -> {

				if (checkpointOptions.getCheckpointType().isSynchronous()) {
					setSynchronousSavepointId(checkpointId);

					if (advanceToEndOfTime) {
						advanceToEndOfEventTime();
					}
				}

				// All of the following steps happen as an atomic step from the perspective of barriers and
				// records/watermarks/timers/callbacks.
				// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
				// checkpoint alignments

				// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
				//           The pre-barrier work should be nothing or minimal in the common case.
				operatorChain.prepareSnapshotPreBarrier(checkpointId);

				// Step (2): Send the checkpoint barrier downstream
				operatorChain.broadcastCheckpointBarrier(
						checkpointId,
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				// Step (3): Take the state snapshot. This should be largely asynchronous, to not
				//           impact progress of the streaming topology
				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);

			});

			return true;
		} else {
			actionExecutor.runThrowing(() -> {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				recordWriter.broadcastEvent(message);
			});

			return false;
		}
	}
```
主要做三件事：1）checkpoint的准备操作，这里通常不进行太多操作；2）发送 CheckpointBarrier；3）存储检查点快照。
#### 广播Barrier

```java
public void broadcastCheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) throws IOException {
		CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, checkpointOptions);
		for (RecordWriterOutput<?> streamOutput : streamOutputs) {
			streamOutput.broadcastEvent(barrier);
		}
	}
```

#### 进行快照
```java
private void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {
		//checkpoint的存储地址及元数据信息
		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
				checkpointMetaData.getCheckpointId(),
				checkpointOptions.getTargetLocation());
		//将checkpoint的过程封装为CheckpointingOperation对象
		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			storage,
			checkpointMetrics);

		checkpointingOperation.executeCheckpointing();
	}
```
每一个算子的快照被抽象为 OperatorSnapshotFutures，包含了 operator state 和 keyed state 的快照结果：

```java
public class OperatorSnapshotFutures {

	@Nonnull
	private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture;

	@Nonnull
	private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture;

	@Nonnull
	private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture;

	@Nonnull
	private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture;
	}
```
由于每一个 StreamTask 可能包含多个算子，因而内部使用一个 Map 维护 OperatorID -> OperatorSnapshotFutures 的关系。

```java
		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;
```
快照的过程分同步和异步两个部分

```java
public void executeCheckpointing() throws Exception {
			startSyncPartNano = System.nanoTime();

			try {
			//同步
				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperator(op);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
						checkpointMetaData.getCheckpointId(), owner.getName());
				}

				startAsyncPartNano = System.nanoTime();

				checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
				//异步
				// checkpoint 可以配置成同步执行，也可以配置成异步执行的
				// 如果是同步执行的，在这里实际上所有的 runnable future 都是已经完成的状态
				AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
					owner,
					operatorSnapshotsInProgress,
					checkpointMetaData,
					checkpointMetrics,
					startAsyncPartNano);

				owner.cancelables.registerCloseable(asyncCheckpointRunnable);
				owner.asyncOperationsThreadPool.execute(asyncCheckpointRunnable);

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}
			} catch (Exception ex) {
				// Cleanup to release resources
				for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
					if (null != operatorSnapshotResult) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception e) {
							LOG.warn("Could not properly cancel an operator snapshot result.", e);
						}
					}
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - did NOT finish synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}

				if (checkpointOptions.getCheckpointType().isSynchronous()) {
					// in the case of a synchronous checkpoint, we always rethrow the exception,
					// so that the task fails.
					// this is because the intention is always to stop the job after this checkpointing
					// operation, and without the failure, the task would go back to normal execution.
					throw ex;
				} else {
					owner.getEnvironment().declineCheckpoint(checkpointMetaData.getCheckpointId(), ex);
				}
			}
		}
```
在同步执行阶段，会依次调用每一个算子的 StreamOperator.snapshotState，返回结果是一个 runnable future。根据 checkpoint 配置成同步模式和异步模式的区别，这个 future 可能处于完成状态，也可能处于未完成状态：

```java
private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {
//同步过程调用算子的snapshotState方法，返回OperatorSnapshotFutures可能已完成或未完成
				OperatorSnapshotFutures snapshotInProgress = op.snapshotState(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions,
						storageLocation);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}
		}
```
详细过程在AbstractStreamOperator#snapshotState

```java
public final OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions,
			CheckpointStreamFactory factory) throws Exception {

		KeyGroupRange keyGroupRange = null != keyedStateBackend ?
				keyedStateBackend.getKeyGroupRange() : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

		OperatorSnapshotFutures snapshotInProgress = new OperatorSnapshotFutures();

		StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl(
			checkpointId,
			timestamp,
			factory,
			keyGroupRange,
			getContainingTask().getCancelables());

		try {
			//对状态进行快照，包括KeyedState和OperatorState
			snapshotState(snapshotContext);

			snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
			snapshotInProgress.setOperatorStateRawFuture(snapshotContext.getOperatorStateStreamFuture());

			//写入operatorState快照
			if (null != operatorStateBackend) {
				snapshotInProgress.setOperatorStateManagedFuture(
					operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}
			//写入keyedState快照	
			if (null != keyedStateBackend) {
				snapshotInProgress.setKeyedStateManagedFuture(
					keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}
		} catch (Exception snapshotException) {
			try {
				snapshotInProgress.cancel();
			} catch (Exception e) {
				snapshotException.addSuppressed(e);
			}

			String snapshotFailMessage = "Could not complete snapshot " + checkpointId + " for operator " +
				getOperatorName() + ".";

			if (!getContainingTask().isCanceled()) {
				LOG.info(snapshotFailMessage, snapshotException);
			}
			try {
				snapshotContext.closeExceptionally();
			} catch (IOException e) {
				snapshotException.addSuppressed(e);
			}
			throw new CheckpointException(snapshotFailMessage, CheckpointFailureReason.CHECKPOINT_DECLINED, snapshotException);
		}

		return snapshotInProgress;
	}
```
我们知道state还分为raw state（原生state）和managed state（flink管理的state），timer定时器属于raw state，也需要写到snapshot中。

```java
/**
	 * Stream operators with state, which want to participate in a snapshot need to override this hook method.
	 *
	 * @param context context that provides information and means required for taking a snapshot
	 */
	public void snapshotState(StateSnapshotContext context) throws Exception {
		final KeyedStateBackend<?> keyedStateBackend = getKeyedStateBackend();
		//TODO all of this can be removed once heap-based timers are integrated with RocksDB incremental snapshots
		// 所有的 timer 都作为 raw keyed state 写入
		if (keyedStateBackend instanceof AbstractKeyedStateBackend &&
			((AbstractKeyedStateBackend<?>) keyedStateBackend).requiresLegacySynchronousTimerSnapshots()) {

			KeyedStateCheckpointOutputStream out;

			try {
				out = context.getRawKeyedOperatorStateOutput();
			} catch (Exception exception) {
				throw new Exception("Could not open raw keyed operator state stream for " +
					getOperatorName() + '.', exception);
			}

			try {
				KeyGroupsList allKeyGroups = out.getKeyGroupList();
				for (int keyGroupIdx : allKeyGroups) {
					out.startNewKeyGroup(keyGroupIdx);

					timeServiceManager.snapshotStateForKeyGroup(
						new DataOutputViewStreamWrapper(out), keyGroupIdx);
				}
			} catch (Exception exception) {
				throw new Exception("Could not write timer service of " + getOperatorName() +
					" to checkpoint state stream.", exception);
			} finally {
				try {
					out.close();
				} catch (Exception closeException) {
					LOG.warn("Could not close raw keyed operator state stream for {}. This " +
						"might have prevented deleting some state data.", getOperatorName(), closeException);
				}
			}
		}
	}
```
上面是AbstractStreamOperator中的snapshotState做的操作，还有个子类AbstractUdfStreamOperator

```java
public void snapshotState(StateSnapshotContext context) throws Exception {
		//先调用父类方法，写入timer
		super.snapshotState(context);
		StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), userFunction);
	}
```

```java
public static void snapshotFunctionState(
			StateSnapshotContext context,
			OperatorStateBackend backend,
			Function userFunction) throws Exception {

		Preconditions.checkNotNull(context);
		Preconditions.checkNotNull(backend);

		while (true) {

			if (trySnapshotFunctionState(context, backend, userFunction)) {
				break;
			}

			// inspect if the user function is wrapped, then unwrap and try again if we can snapshot the inner function
			if (userFunction instanceof WrappingFunction) {
				userFunction = ((WrappingFunction<?>) userFunction).getWrappedFunction();
			} else {
				break;
			}
		}
	}


private static boolean trySnapshotFunctionState(
			StateSnapshotContext context,
			OperatorStateBackend backend,
			Function userFunction) throws Exception {
		//如果用户函数实现了CheckpointedFunction接口，则调用udf中的snapshotState方法进行快照
		if (userFunction instanceof CheckpointedFunction) {
			((CheckpointedFunction) userFunction).snapshotState(context);

			return true;
		}
		// 如果用户函数实现了 ListCheckpointed
		if (userFunction instanceof ListCheckpointed) {
		//先调用 snapshotState 方法获取当前状态
			@SuppressWarnings("unchecked")
			List<Serializable> partitionableState = ((ListCheckpointed<Serializable>) userFunction).
					snapshotState(context.getCheckpointId(), context.getCheckpointTimestamp());
			//获取状态后端存储引用
			ListState<Serializable> listState = backend.
					getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);
			//清空
			listState.clear();
			//当前状态写入状态后端存储
			if (null != partitionableState) {
				try {
					for (Serializable statePartition : partitionableState) {
						listState.add(statePartition);
					}
				} catch (Exception e) {
					listState.clear();

					throw new Exception("Could not write partitionable state to operator " +
						"state backend.", e);
				}
			}

			return true;
		}

		return false;
	}
```

到这里我们知道了checkpoint过程中如何调用到我们自己实现的快照方法。再看下flink管理的状态是如何写入快照的。

```java
			if (null != operatorStateBackend) {
				snapshotInProgress.setOperatorStateManagedFuture(
					operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}
			
			if (null != keyedStateBackend) {
				snapshotInProgress.setKeyedStateManagedFuture(
					keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}
```
首先来看看 operator state。DefaultOperatorStateBackend 将实际的工作交给 DefaultOperatorStateBackendSnapshotStrategy 完成。首先，会为对当前注册的所有 operator state（包含 list state 和 broadcast state）做深度拷贝，然后将实际的写入操作封装在一个异步的 FutureTask 中，这个 FutureTask 的主要任务包括： 1）打开输出流 2）写入状态元数据信息 3）写入状态 4）关闭输出流，获得状态句柄。如果不启用异步checkpoint模式，那么这个 FutureTask 在同步阶段就会立刻执行。

```java
public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
		final long checkpointId,
		final long timestamp,
		@Nonnull final CheckpointStreamFactory streamFactory,
		@Nonnull final CheckpointOptions checkpointOptions) throws IOException {

		if (registeredOperatorStates.isEmpty() && registeredBroadcastStates.isEmpty()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		final Map<String, PartitionableListState<?>> registeredOperatorStatesDeepCopies =
			new HashMap<>(registeredOperatorStates.size());
		final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStatesDeepCopies =
			new HashMap<>(registeredBroadcastStates.size());

		ClassLoader snapshotClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(userClassLoader);
		try {
			// eagerly create deep copies of the list and the broadcast states (if any)
			// in the synchronous phase, so that we can use them in the async writing.
			//获得已注册的所有 list state 和 broadcast state 的深拷贝
			if (!registeredOperatorStates.isEmpty()) {
				for (Map.Entry<String, PartitionableListState<?>> entry : registeredOperatorStates.entrySet()) {
					PartitionableListState<?> listState = entry.getValue();
					if (null != listState) {
						listState = listState.deepCopy();
					}
					registeredOperatorStatesDeepCopies.put(entry.getKey(), listState);
				}
			}

			if (!registeredBroadcastStates.isEmpty()) {
				for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry : registeredBroadcastStates.entrySet()) {
					BackendWritableBroadcastState<?, ?> broadcastState = entry.getValue();
					if (null != broadcastState) {
						broadcastState = broadcastState.deepCopy();
					}
					registeredBroadcastStatesDeepCopies.put(entry.getKey(), broadcastState);
				}
			}
		} finally {
			Thread.currentThread().setContextClassLoader(snapshotClassLoader);
		}
//将主要写入操作封装为一个异步的FutureTask
		AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>> snapshotCallable =
			new AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>>() {

				@Override
				protected SnapshotResult<OperatorStateHandle> callInternal() throws Exception {
					// 创建状态输出流
					CheckpointStreamFactory.CheckpointStateOutputStream localOut =
						streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
					snapshotCloseableRegistry.registerCloseable(localOut);
					// 收集元数据
					// get the registered operator state infos ...
					List<StateMetaInfoSnapshot> operatorMetaInfoSnapshots =
						new ArrayList<>(registeredOperatorStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredOperatorStatesDeepCopies.entrySet()) {
						operatorMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}
					// 写入元数据
					// ... get the registered broadcast operator state infos ...
					List<StateMetaInfoSnapshot> broadcastMetaInfoSnapshots =
						new ArrayList<>(registeredBroadcastStatesDeepCopies.size());

					for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry :
						registeredBroadcastStatesDeepCopies.entrySet()) {
						broadcastMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}
		
					// ... write them all in the checkpoint stream ...
					DataOutputView dov = new DataOutputViewStreamWrapper(localOut);

					OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(operatorMetaInfoSnapshots, broadcastMetaInfoSnapshots);

					backendSerializationProxy.write(dov);

					// ... and then go for the states ...
					// 写入状态
					// we put BOTH normal and broadcast state metadata here
					int initialMapCapacity =
						registeredOperatorStatesDeepCopies.size() + registeredBroadcastStatesDeepCopies.size();
					final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
						new HashMap<>(initialMapCapacity);

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredOperatorStatesDeepCopies.entrySet()) {

						PartitionableListState<?> value = entry.getValue();
						long[] partitionOffsets = value.write(localOut);
						OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
						writtenStatesMetaData.put(
							entry.getKey(),
							new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					// ... and the broadcast states themselves ...
					for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry :
						registeredBroadcastStatesDeepCopies.entrySet()) {

						BackendWritableBroadcastState<?, ?> value = entry.getValue();
						long[] partitionOffsets = {value.write(localOut)};
						OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
						writtenStatesMetaData.put(
							entry.getKey(),
							new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					// ... and, finally, create the state handle.
					OperatorStateHandle retValue = null;

					if (snapshotCloseableRegistry.unregisterCloseable(localOut)) {
						//关闭输出流，获得状态句柄，后面可以用这个句柄读取状态
						StreamStateHandle stateHandle = localOut.closeAndGetHandle();

						if (stateHandle != null) {
							retValue = new OperatorStreamStateHandle(writtenStatesMetaData, stateHandle);
						}

						return SnapshotResult.of(retValue);
					} else {
						throw new IOException("Stream was already unregistered.");
					}
				}

				@Override
				protected void cleanupProvidedResources() {
					// nothing to do
				}

				@Override
				protected void logAsyncSnapshotComplete(long startTime) {
					if (asynchronousSnapshots) {
						logAsyncCompleted(streamFactory, startTime);
					}
				}
			};

		final FutureTask<SnapshotResult<OperatorStateHandle>> task =
			snapshotCallable.toAsyncSnapshotFutureTask(closeStreamOnCancelRegistry);
//如果不是异步 checkpoint 那么在这里直接运行 FutureTask，即在同步阶段就完成了状态的写入
		if (!asynchronousSnapshots) {
			task.run();
		}

		return task;
	}
```
keyed state 写入的基本流程与此相似，但由于 keyed state 在存储时有多种实现，包括基于堆内存和 RocksDB 的不同实现，此外基于 RocksDB 的实现还包括支持增量 checkpoint，因而相比于 operator state 要更复杂一些。

至此，我们介绍了快照操作的第一个阶段，即同步执行的阶段。异步执行阶段被封装为 AsyncCheckpointRunnable，主要的操作包括 1）执行同步阶段创建的 FutureTask 2）完成后向 CheckpointCoordinator 发送 Ack 响应。

```java
protected static final class AsyncCheckpointRunnable implements Runnable, Closeable {
		@Override
		public void run() {
			FileSystemSafetyNet.initializeSafetyNetForThread();
			try {
				TaskStateSnapshot jobManagerTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());
				TaskStateSnapshot localTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				// 完成每一个 operator 的状态写入
				// 如果是同步 checkpoint，那么在此之前状态已经写入完成
				// 如果是异步 checkpoint，那么在这里才会写入状态
				for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry : operatorSnapshotsInProgress.entrySet()) {
					OperatorID operatorID = entry.getKey();
					OperatorSnapshotFutures snapshotInProgress = entry.getValue();
					// finalize the async part of all by executing all snapshot runnables
					OperatorSnapshotFinalizer finalizedSnapshots =
						new OperatorSnapshotFinalizer(snapshotInProgress);

					jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getJobManagerOwnedState());

					localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getTaskLocalState());
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000L;

				checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsyncCheckpointState.RUNNING,
					CheckpointingOperation.AsyncCheckpointState.COMPLETED)) {
					//报告 snapshot 完成
					reportCompletedSnapshotStates(
						jobManagerTaskOperatorSubtaskStates,
						localTaskOperatorSubtaskStates,
						asyncDurationMillis);

				} else {
					LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
						owner.getName(),
						checkpointMetaData.getCheckpointId());
				}
			} catch (Exception e) {
				handleExecutionException(e);
			} finally {
				owner.cancelables.unregisterCloseable(this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
			}
		}
	}

	private void reportCompletedSnapshotStates(
			TaskStateSnapshot acknowledgedTaskStateSnapshot,
			TaskStateSnapshot localTaskStateSnapshot,
			long asyncDurationMillis) {
			TaskStateManager taskStateManager = owner.getEnvironment().getTaskStateManager();
			boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
			boolean hasLocalState = localTaskStateSnapshot.hasState();
			// we signal stateless tasks by reporting null, so that there are no attempts to assign empty state
			// to stateless tasks on restore. This enables simple job modifications that only concern
			// stateless without the need to assign them uids to match their (always empty) states.
			taskStateManager.reportTaskStateSnapshots(
				checkpointMetaData,
				checkpointMetrics,
				hasAckState ? acknowledgedTaskStateSnapshot : null,
				hasLocalState ? localTaskStateSnapshot : null);
		}
}

public class TaskStateManagerImpl implements TaskStateManager {
	@Override
	public void reportTaskStateSnapshots(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nullable TaskStateSnapshot acknowledgedState,
		@Nullable TaskStateSnapshot localState) {

		long checkpointId = checkpointMetaData.getCheckpointId();

		localStateStore.storeLocalState(checkpointId, localState);

		//发送 ACK 响应给 CheckpointCoordinator
		checkpointResponder.acknowledgeCheckpoint(
			jobId,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			acknowledgedState);
	}
}
```
#### Checkpoint 的确认
Task 对 checkpoint 的响应是通过 CheckpointResponder 接口完成的：

```java
public interface CheckpointResponder {

	/**
	 * Acknowledges the given checkpoint.
	 */
	void acknowledgeCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		CheckpointMetrics checkpointMetrics,
		TaskStateSnapshot subtaskState);

	/**
	 * Declines the given checkpoint.
	 */
	void declineCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		Throwable cause);
}
```
RpcCheckpointResponder 作为 CheckpointResponder 的具体实现，主要是通过 RPC 调用通知 CheckpointCoordinatorGateway，即通知给 JobMaster, JobMaster 调用 CheckpointCoordinator.receiveAcknowledgeMessage() 和 CheckpointCoordinator.receiveDeclineMessage() 进行处理。
#### 确认完成
在一个 Task 完成 checkpoint 操作后，CheckpointCoordinator 接收到 Ack 响应，对 Ack 响应的处理流程主要如下：

- 根据 Ack 的 checkpointID 从 Map<Long, PendingCheckpoint> pendingCheckpoints 中查找对应的 PendingCheckpoint
- 若存在对应的 PendingCheckpoint
 	- 这个 PendingCheckpoint 没有被丢弃，调用 PendingCheckpoint.acknowledgeTask 方法处理 Ack，根据处理结果的不同：
 		- SUCCESS：判断是否已经接受了所有需要响应的 Ack，如果是，则调用 completePendingCheckpoint 完成此次 checkpoint
 		- DUPLICATE：Ack 消息重复接收，直接忽略
		- UNKNOWN：未知的 Ack 消息，清理上报的 Ack 中携带的状态句柄
		- DISCARD：Checkpoint 已经被 discard，清理上报的 Ack 中携带的状态句柄
	- 这个 PendingCheckpoint 已经被丢弃，抛出异常
- 若不存在对应的 PendingCheckpoint，则清理上报的 Ack 中携带的状态句柄
相应代码：

```java
class CheckpointCoordinator {
	public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message) throws CheckpointException {
		if (shutdown || message == null) {
			return false;
		}

		if (!job.equals(message.getJob())) {
			LOG.error("Received wrong AcknowledgeCheckpoint message for job {}: {}", job, message);
			return false;
		}

		final long checkpointId = message.getCheckpointId();

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return false;
			}

			final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {

				switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getSubtaskState(), message.getCheckpointMetrics())) {
					case SUCCESS:
						LOG.debug("Received acknowledge message for checkpoint {} from task {} of job {}.",
							checkpointId, message.getTaskExecutionId(), message.getJob());

						if (checkpoint.isFullyAcknowledged()) {
							completePendingCheckpoint(checkpoint);
						}
						break;
					case DUPLICATE:
						LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob());
						break;
					case UNKNOWN:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {}, " +
								"because the task's execution attempt id was unknown. Discarding " +
								"the state handle to avoid lingering state.", message.getCheckpointId(),
							message.getTaskExecutionId(), message.getJob());

						discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

						break;
					case DISCARDED:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {}, " +
							"because the pending checkpoint had been discarded. Discarding the " +
								"state handle tp avoid lingering state.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob());

						discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());
				}

				return true;
			}
			else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
						"Received message for discarded but non-removed checkpoint " + checkpointId);
			}
			else {
				boolean wasPendingCheckpoint;
				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				if (recentPendingCheckpoints.contains(checkpointId)) {
					wasPendingCheckpoint = true;
					LOG.warn("Received late message for now expired checkpoint attempt {} from " +
						"{} of job {}.", checkpointId, message.getTaskExecutionId(), message.getJob());
				}
				else {
					LOG.debug("Received message for an unknown checkpoint {} from {} of job {}.",
						checkpointId, message.getTaskExecutionId(), message.getJob());
					wasPendingCheckpoint = false;
				}

				// try to discard the state so that we don't have lingering state lying around
				discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

				return wasPendingCheckpoint;
			}
		}
	}
}
```
对于一个已经触发但还没有完成的 checkpoint，即 PendingCheckpoint，它是如何处理 Ack 消息的呢？在 PendingCheckpoint 内部维护了两个 Map，分别是：

- Map<OperatorID, OperatorState> operatorStates; : 已经接收到 Ack 的算子的状态句柄
- Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;: 需要 Ack 但还没有接收到的 Task

每当接收到一个 Ack 消息时，PendingCheckpoint 就从 notYetAcknowledgedTasks 中移除对应的 Task，并保存 Ack 携带的状态句柄保存。当 notYetAcknowledgedTasks 为空时，表明所有的 Ack 消息都接收到了。

一旦 PendingCheckpoint 确认所有 Ack 消息都已经接收，那么就可以完成此次 checkpoint 了，具体包括：

- 调用 PendingCheckpoint.finalizeCheckpoint() 将 PendingCheckpoint 转化为 CompletedCheckpoint
	- 获取 CheckpointMetadataOutputStream，将所有的状态句柄信息通过 CheckpointMetadataOutputStream 写入到存储系统中
	- 创建一个 CompletedCheckpoint 对象
- 将 CompletedCheckpoint 保存到 CompletedCheckpointStore 中
	- CompletedCheckpointStore 有两种实现，分别为 StandaloneCompletedCheckpointStore 和 ZooKeeperCompletedCheckpointStore
	- StandaloneCompletedCheckpointStore 简单地将 CompletedCheckpointStore 存放在一个数组中
	- ZooKeeperCompletedCheckpointStore 提供高可用实现：先将 CompletedCheckpointStore 写入到 RetrievableStateStorageHelper 中（通常是文件系统），然后将文件句柄存在 ZK 中
	- 保存的 CompletedCheckpointStore 数量是有限的，会删除旧的快照
- 移除被越过的 PendingCheckpoint，因为 CheckpointID 是递增的，那么所有比当前完成的 CheckpointID 小的 PendingCheckpoint 都可以被丢弃了
- 依次调用 Execution.notifyCheckpointComplete() 通知所有的 Task 当前 Checkpoint 已经完成
	- 通过 RPC 调用 TaskExecutor.confirmCheckpoint() 告知对应的 Task

Task收到notifyCheckpointComplete确认后进行后续处理，比如kafkaproduce的两段式提交过程。

### 总结
本文分析了checkpoint进行snapshot的过程，包括广播barrier、进行snapshot以及checkpoint完成后的ACK过程。
