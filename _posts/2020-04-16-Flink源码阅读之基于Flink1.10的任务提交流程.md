---
layout:     post
title:      Flink源码阅读之基于Flink1.10的任务提交流程
subtitle:   Flink源码阅读之基于Flink1.10的任务提交流程
date:       2020-04-16
author:     Sun.Zhu
header-img: img/057.jpg
catalog: true
tags:
    - Flink
    - 提交流程
---

Flink在1.10版本对整个作业提交流程有了较大改动，详情请见[FLIP-73](https://cwiki.apache.org/confluence/display/FLINK/FLIP-73:+Introducing+Executors+for+job+submission)。本文基于1.10对作业提交的关键流程进行分析，不深究。
**入口：** 依旧是main函数最后env.execute();

```java
public JobExecutionResult execute(String jobName) throws Exception {
		Preconditions.checkNotNull(jobName, "Streaming Job name should not be null.");

		return execute(getStreamGraph(jobName));//构造streamGraph
	}
	
public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		final JobClient jobClient = executeAsync(streamGraph);//异步执行，返回JobClient对象
		//......省略部分代码
}
```
根据execution.target配置获取对应PipelineExecutorFactory

```java
public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
		checkNotNull(streamGraph, "StreamGraph cannot be null.");
		checkNotNull(configuration.get(DeploymentOptions.TARGET), "No execution.target specified in your configuration file.");

		final PipelineExecutorFactory executorFactory =
			executorServiceLoader.getExecutorFactory(configuration);

		checkNotNull(
			executorFactory,
			"Cannot find compatible factory for specified execution.target (=%s)",
			configuration.get(DeploymentOptions.TARGET));

		CompletableFuture<JobClient> jobClientFuture = executorFactory
			.getExecutor(configuration)//PipelineExecutor
			.execute(streamGraph, configuration);
			//......
}
```
PipelineExecutor接口有多种实现，以LocalExecutor为例，Pipeline是一个空接口为了把 StreamGraph(stream 程序) 和 Plan (batch 程序)抽象到一起。

```java
public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration) throws Exception {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		// we only support attached execution with the local executor.
		checkState(configuration.getBoolean(DeploymentOptions.ATTACHED));

		final JobGraph jobGraph = getJobGraph(pipeline, configuration);//根据streamGraph生成jobGraph
		final MiniCluster miniCluster = startMiniCluster(jobGraph, configuration);//启动MiniCluster （这个启动过程比较复杂，包括启动rpcservice、haservice、taskmanager、resourcemanager等）
		final MiniClusterClient clusterClient = new MiniClusterClient(configuration, miniCluster);

		CompletableFuture<JobID> jobIdFuture = clusterClient.submitJob(jobGraph);//通过MiniClusterClient提交job

		jobIdFuture
				.thenCompose(clusterClient::requestJobResult)
				.thenAccept((jobResult) -> clusterClient.shutDownCluster());

		return jobIdFuture.thenApply(jobID ->
				new ClusterClientJobClientAdapter<>(() -> clusterClient, jobID));
	}
```
通过RPC方式提交jobGraph

```java
public CompletableFuture<JobSubmissionResult> submitJob(JobGraph jobGraph) {
		//......
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture
			.thenCombine(
				dispatcherGatewayFuture,
				(Void ack, DispatcherGateway dispatcherGateway) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout))
			.thenCompose(Function.identity());
		//......
	}
public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		//......
				return internalSubmitJob(jobGraph);
		//.......
	}
private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
		final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
			.thenApply(ignored -> Acknowledge.get());
	//......
}
private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) throws Exception {
		jobGraphWriter.putJobGraph(jobGraph);

		final CompletableFuture<Void> runJobFuture = runJob(jobGraph);//run job
}

```
创建JobManagerRunner

```java
private CompletableFuture<Void> runJob(JobGraph jobGraph) {
		Preconditions.checkState(!jobManagerRunnerFutures.containsKey(jobGraph.getJobID()));
		//创建JobManagerRunner
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);
		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);

		return jobManagerRunnerFuture
			.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable != null) {
						jobManagerRunnerFutures.remove(jobGraph.getJobID());
					}
				},
				getMainThreadExecutor());
}
private JobManagerRunner startJobManagerRunner(JobManagerRunner jobManagerRunner) throws Exception {
	//......
	jobManagerRunner.start();
	//......
}

```
启动的 JobManagerRunner 会竞争 leader ，一旦被选举为 leader，就会启动一个 JobMaster。

```java
@Override
	public void start() throws Exception {
		try {
			leaderElectionService.start(this);
		} catch (Exception e) {
			log.error("Could not start the JobManager because the leader election service did not start.", e);
			throw new Exception("Could not start the leader election service.", e);
		}
	}
public void start(LeaderContender contender) throws Exception {
			checkNotNull(contender);
			addContender(this, contender);//参与leader竞争
		}
/**
	 * Callback from leader contenders when they start their service.
	 */
	private void addContender(EmbeddedLeaderElectionService service, LeaderContender contender) {
		//......
			if (!allLeaderContenders.add(service)) {//添加到竞争者集合
					throw new IllegalStateException("leader election service was added to this service multiple times");
				}
		//一旦竞争leader成功，则更新leader
		updateLeader().whenComplete((aVoid, throwable) -> {
					if (throwable != null) {
						fatalError(throwable);
					}
				});
		//.......
}
private CompletableFuture<Void> updateLeader() {
//.......
		if (allLeaderContenders.isEmpty()) {
				// no new leader available, tell everyone that there is no leader currently
				return notifyAllListeners(null, null);
			}
			else {//上面已经添加了一个竞争者，走else逻辑
				// propose a leader and ask it
				final UUID leaderSessionId = UUID.randomUUID();//生成leaderSessionId 
				EmbeddedLeaderElectionService leaderService = allLeaderContenders.iterator().next();

				currentLeaderSessionId = leaderSessionId;
				currentLeaderProposed = leaderService;
				currentLeaderProposed.isLeader = true;

				LOG.info("Proposing leadership to contender {}", leaderService.contender.getDescription());

				return execute(new GrantLeadershipCall(leaderService.contender, leaderSessionId, LOG));//启动一个线程
			}
}
private static class GrantLeadershipCall implements Runnable {
public void run() {
			try {
				contender.grantLeadership(leaderSessionId);
			}
			catch (Throwable t) {
				logger.warn("Error granting leadership to contender", t);
				contender.handleError(t instanceof Exception ? (Exception) t : new Exception(t));
			}
		}
}

public void grantLeadership(final UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			leadershipOperation = leadershipOperation.thenCompose(
				(ignored) -> {
					synchronized (lock) {
						return verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);//启动jobManager
					}
				});

			handleException(leadershipOperation, "Could not start the job manager.");
		}
	}
private CompletableFuture<Void> verifyJobSchedulingStatusAndStartJobManager(UUID leaderSessionId) {
		final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();

		return jobSchedulingStatusFuture.thenCompose(
			jobSchedulingStatus -> {
				if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
					return jobAlreadyDone();
				} else {
					return startJobMaster(leaderSessionId);
				}
			});
	}

	private CompletionStage<Void> startJobMaster(UUID leaderSessionId) {
		log.info("JobManager runner for job {} ({}) was granted leadership with session id {} at {}.",
			jobGraph.getName(), jobGraph.getJobID(), leaderSessionId, jobMasterService.getAddress());

		try {
			runningJobsRegistry.setJobRunning(jobGraph.getJobID());
		} catch (IOException e) {
			return FutureUtils.completedExceptionally(
				new FlinkException(
					String.format("Failed to set the job %s to running in the running jobs registry.", jobGraph.getJobID()),
					e));
		}

		final CompletableFuture<Acknowledge> startFuture;
		try {
			startFuture = jobMasterService.start(new JobMasterId(leaderSessionId));//启动 JobMaster
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(new FlinkException("Failed to start the JobMaster.", e));
		}

		final CompletableFuture<JobMasterGateway> currentLeaderGatewayFuture = leaderGatewayFuture;
		return startFuture.thenAcceptAsync(
			(Acknowledge ack) -> confirmLeaderSessionIdIfStillLeader(
				leaderSessionId,
				jobMasterService.getAddress(),
				currentLeaderGatewayFuture),
			executor);
	}
```
JobMaster启动之后会和ResourceManager通信

```java
public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {
		// make sure we receive RPC and async calls
		start();

		return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
	}
private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {

		//.......

		startJobMasterServices();
		resetAndStartScheduler();//这里开始正式的任务调度过程了

		//......
	}
private void startJobMasterServices() throws Exception {
		startHeartbeatServices();

		// start the slot pool make sure the slot pool now accepts messages for this leader
		slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
		scheduler.start(getMainThreadExecutor());

		//TODO: Remove once the ZooKeeperLeaderRetrieval returns the stored address upon start
		// try to reconnect to previously known leader
		reconnectToResourceManager(new FlinkException("Starting JobMaster component."));//和ResourceManager建立连接

		// job is ready to go, try to establish connection with resource manager
		//   - activate leader retrieval for the resource manager
		//   - on notification of the leader, the connection will be established and
		//     the slot pool will start requesting slots
		resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
	}
```
之后就是任务的调度过程了，下次再说。

**总结：** 本文主要分析了flink job提交的流程。从streamgraph、jobgraph生成，启动MiniCluster，通过RPC把jobGraph提交给Dispacher，然后启动jobMaster竞争leader并和ResourceManager建立通信。

附上一段FlinkSQL任务的日志，可以看到任务提交的大概流程

> 2020-04-18 11:42:43,500 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         - --------------------------------------------------------------------------------
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  Starting StandaloneSessionClusterEntrypoint (Version: 1.10.0, Rev:aa4eb8f, Date:07.02.2020 @ 19:18:19 CET)
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  OS current user: xxxxxx
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  Current Hadoop/Kerberos user: <no hadoop dependency found>
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  JVM: Java HotSpot(TM) 64-Bit Server VM - Oracle Corporation - 1.8/25.241-b07
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  Maximum heap size: 981 MiBytes
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  JAVA_OME: (not set)
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  No HadHoop Dependency available
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  JVM Options:
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     -Xms1024m
2020-04-18 11:42:43,502 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     -Xmx1024m
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     -Dlog.file=/Users/xxxxxx/Desktop/software/flink-1.10.0/log/flink-xxxxxx-standalonesession-0-xxxxxxdeMacBook-Pro.local.log
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     -Dlog4j.configuration=file:/Users/xxxxxx/Desktop/software/flink-1.10.0/conf/log4j.properties
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     -Dlogback.configurationFile=file:/Users/xxxxxx/Desktop/software/flink-1.10.0/conf/logback.xml
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  Program Arguments:
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     --configDir
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     /Users/xxxxxx/Desktop/software/flink-1.10.0/conf
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     --executionMode
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -     cluster
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         -  Classpath: /Users/xxxxxx/Desktop/software/flink-1.10.0/lib/flink-jdbc_2.11-1.10.0.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/flink-json-1.10.0.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/flink-sql-connector-elasticsearch6_2.11-1.10.0.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/flink-sql-connector-kafka_2.11-1.10.0.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/flink-table-blink_2.11-1.10.0.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/flink-table_2.11-1.10.0.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/log4j-1.2.17.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/mysql-connector-java-5.1.48.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/slf4j-log4j12-1.7.15.jar:/Users/xxxxxx/Desktop/software/flink-1.10.0/lib/flink-dist_2.11-1.10.0.jar:::
2020-04-18 11:42:43,503 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         - --------------------------------------------------------------------------------
2020-04-18 11:42:43,505 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         - Registered UNIX signal handlers for [TERM, HUP, INT]
2020-04-18 11:42:43,521 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.address, localhost
2020-04-18 11:42:43,521 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.port, 6123
2020-04-18 11:42:43,522 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.heap.size, 1024m
2020-04-18 11:42:43,522 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.memory.process.size, 1568m
2020-04-18 11:42:43,522 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.numberOfTaskSlots, 10
2020-04-18 11:42:43,522 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: parallelism.default, 1
2020-04-18 11:42:43,522 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.execution.failover-strategy, region
2020-04-18 11:42:43,604 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         - Starting StandaloneSessionClusterEntrypoint.
2020-04-18 11:42:43,604 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         - Install default filesystem.
2020-04-18 11:42:43,658 INFO  org.apache.flink.core.fs.FileSystem                           - Hadoop is not in the classpath/dependencies. The extended set of supported File Systems via Hadoop is not available.
2020-04-18 11:42:43,883 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         - Install security context.
2020-04-18 11:42:43,894 INFO  org.apache.flink.runtime.security.modules.HadoopModuleFactory  - Cannot create Hadoop Security Module because Hadoop cannot be found in the Classpath.
2020-04-18 11:42:43,907 INFO  org.apache.flink.runtime.security.modules.JaasModule          - Jaas file will be created as /var/folders/2r/lpp1j14x5vx4_fj6n2qdht2w0000gn/T/jaas-8583684339714096047.conf.
2020-04-18 11:42:43,911 INFO  org.apache.flink.runtime.security.SecurityUtils               - Cannot install HadoopSecurityContext because Hadoop cannot be found in the Classpath.
2020-04-18 11:42:43,911 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint         - Initializing cluster services.
2020-04-18 11:42:43,950 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils         - Trying to start actor system at localhost:6123
2020-04-18 11:42:44,489 INFO  akka.event.slf4j.Slf4jLogger                                  - Slf4jLogger started
2020-04-18 11:42:44,512 INFO  akka.remote.Remoting                                          - Starting remoting
2020-04-18 11:42:44,646 INFO  akka.remote.Remoting                                          - Remoting started; listening on addresses :[akka.tcp://flink@localhost:6123]
2020-04-18 11:42:44,759 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils         - Actor system started at akka.tcp://flink@localhost:6123
2020-04-18 11:42:44,774 INFO  org.apache.flink.configuration.Configuration                  - Config uses fallback configuration key 'jobmanager.rpc.address' instead of key 'rest.address'
2020-04-18 11:42:44,781 INFO  org.apache.flink.runtime.blob.BlobServer                      - Created BLOB server storage directory /var/folders/2r/lpp1j14x5vx4_fj6n2qdht2w0000gn/T/blobStore-974c4b61-96f4-4dea-8e45-82b74478dc6a
2020-04-18 11:42:44,790 INFO  org.apache.flink.runtime.blob.BlobServer                      - Started BLOB server at 0.0.0.0:49811 - max concurrent requests: 50 - max backlog: 1000
2020-04-18 11:42:44,802 INFO  org.apache.flink.runtime.metrics.MetricRegistryImpl           - No metrics reporter configured, no metrics will be exposed/reported.
2020-04-18 11:42:44,806 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils         - Trying to start actor system at localhost:0
2020-04-18 11:42:44,823 INFO  akka.event.slf4j.Slf4jLogger                                  - Slf4jLogger started
2020-04-18 11:42:44,826 INFO  akka.remote.Remoting                                          - Starting remoting
2020-04-18 11:42:44,831 INFO  akka.remote.Remoting                                          - Remoting started; listening on addresses :[akka.tcp://flink-metrics@localhost:49812]
2020-04-18 11:42:44,839 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils         - Actor system started at akka.tcp://flink-metrics@localhost:49812
2020-04-18 11:42:44,844 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcService              - Starting RPC endpoint for org.apache.flink.runtime.metrics.dump.MetricQueryService at akka://flink-metrics/user/MetricQueryService .
2020-04-18 11:42:44,942 INFO  org.apache.flink.runtime.dispatcher.FileArchivedExecutionGraphStore  - Initializing FileArchivedExecutionGraphStore: Storage directory /var/folders/2r/lpp1j14x5vx4_fj6n2qdht2w0000gn/T/executionGraphStore-5064e3ab-a7a4-407f-8897-3fe5b48bb58f, expiration time 3600000, maximum cache size 52428800 bytes.
2020-04-18 11:42:44,984 INFO  org.apache.flink.configuration.Configuration                  - Config uses fallback configuration key 'jobmanager.rpc.address' instead of key 'rest.address'
2020-04-18 11:42:44,986 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint    - Upload directory /var/folders/2r/lpp1j14x5vx4_fj6n2qdht2w0000gn/T/flink-web-1d80c9c1-129b-4f01-a285-e18924776063/flink-web-upload does not exist. 
2020-04-18 11:42:44,986 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint    - Created directory /var/folders/2r/lpp1j14x5vx4_fj6n2qdht2w0000gn/T/flink-web-1d80c9c1-129b-4f01-a285-e18924776063/flink-web-upload for file uploads.
2020-04-18 11:42:44,988 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint    - Starting rest endpoint.
2020-04-18 11:42:45,238 INFO  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Determined location of main cluster component log file: /Users/xxxxxx/Desktop/software/flink-1.10.0/log/flink-xxxxxx-standalonesession-0-xxxxxxdeMacBook-Pro.local.log
2020-04-18 11:42:45,238 INFO  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Determined location of main cluster component stdout file: /Users/xxxxxx/Desktop/software/flink-1.10.0/log/flink-xxxxxx-standalonesession-0-xxxxxxdeMacBook-Pro.local.out
2020-04-18 11:42:45,421 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint    - Rest endpoint listening at localhost:8081
2020-04-18 11:42:45,422 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint    - http://localhost:8081 was granted leadership with leaderSessionID=00000000-0000-0000-0000-000000000000
2020-04-18 11:42:45,422 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint    - Web frontend listening at http://localhost:8081.
2020-04-18 11:42:45,433 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcService              - Starting RPC endpoint for org.apache.flink.runtime.resourcemanager.StandaloneResourceManager at akka://flink/user/resourcemanager .
2020-04-18 11:42:45,458 INFO  org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess  - Start SessionDispatcherLeaderProcess.
2020-04-18 11:42:45,459 INFO  org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess  - Recover all persisted job graphs.
2020-04-18 11:42:45,459 INFO  org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess  - Successfully recovered 0 persisted job graphs.
2020-04-18 11:42:45,462 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager  - ResourceManager akka.tcp://flink@localhost:6123/user/resourcemanager was granted leadership with fencing token 00000000000000000000000000000000
2020-04-18 11:42:45,465 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcService              - Starting RPC endpoint for org.apache.flink.runtime.dispatcher.StandaloneDispatcher at akka://flink/user/dispatcher .
2020-04-18 11:42:45,466 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerImpl  - Starting the SlotManager.
2020-04-18 11:42:45,858 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager  - Registering TaskManager with ResourceID ddf441898e05993ca96163409ecde9b3 (akka.tcp://flink@127.0.0.1:49813/user/taskmanager_0) at ResourceManager
2020-04-18 11:58:50,389 INFO  org.apache.flink.runtime.dispatcher.StandaloneDispatcher      - Received JobGraph submission eb9e2c44fdcd2f6719188f515ddad7ae (default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)).
2020-04-18 11:58:50,390 INFO  org.apache.flink.runtime.dispatcher.StandaloneDispatcher      - Submitting job eb9e2c44fdcd2f6719188f515ddad7ae (default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)).
2020-04-18 11:58:50,411 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcService              - Starting RPC endpoint for org.apache.flink.runtime.jobmaster.JobMaster at akka://flink/user/jobmanager_0 .
2020-04-18 11:58:50,420 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Initializing job default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) (eb9e2c44fdcd2f6719188f515ddad7ae).
2020-04-18 11:58:50,436 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Using restart back off time strategy NoRestartBackoffTimeStrategy for default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) (eb9e2c44fdcd2f6719188f515ddad7ae).
2020-04-18 11:58:50,468 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Running initialization on master for job default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) (eb9e2c44fdcd2f6719188f515ddad7ae).
2020-04-18 11:58:50,468 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Successfully ran initialization on master in 0 ms.
2020-04-18 11:58:50,499 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)
2020-04-18 11:58:50,512 INFO  org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy  - Start building failover regions.
2020-04-18 11:58:50,512 INFO  org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy  - Created 1 failover regions.
2020-04-18 11:58:50,512 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Using failover strategy org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy@7c9f4190 for default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) (eb9e2c44fdcd2f6719188f515ddad7ae).
2020-04-18 11:58:50,515 INFO  org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl       - JobManager runner for job default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) (eb9e2c44fdcd2f6719188f515ddad7ae) was granted leadership with session id 00000000-0000-0000-0000-000000000000 at akka.tcp://flink@localhost:6123/user/jobmanager_0.
2020-04-18 11:58:50,517 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Starting execution of job default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) (eb9e2c44fdcd2f6719188f515ddad7ae) under job master id 00000000000000000000000000000000.
2020-04-18 11:58:50,519 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Starting scheduling with scheduling strategy [org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy]
2020-04-18 11:58:50,520 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Job default: SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) (eb9e2c44fdcd2f6719188f515ddad7ae) switched from state CREATED to RUNNING.
2020-04-18 11:58:50,529 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: KafkaTableSource(user_id, item_id, category_id, behavior, ts) -> SourceConversion(table=[default_catalog.default_database.user_behavior, source: [KafkaTableSource(user_id, item_id, category_id, behavior, ts)]], fields=[user_id, item_id, category_id, behavior, ts]) -> Calc(select=[user_id, item_id, category_id, behavior, ts, () AS proctime]) -> WatermarkAssigner(rowtime=[ts], watermark=[(ts - 5000:INTERVAL SECOND)]) -> Calc(select=[ts], where=[(behavior = _UTF-16LE'buy':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")]) (1/1) (dab698f7e11df09032d07bec79d8dffd) switched from CREATED to SCHEDULED.
2020-04-18 11:58:50,529 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - GroupWindowAggregate(window=[TumblingGroupWindow('w$, ts, 3600000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[COUNT(*) AS EXPR$1, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime]) -> Calc(select=[(FLAG(HOUR) EXTRACT w$start) AS EXPR$0, EXPR$1]) -> SinkConversionToTuple2 -> Sink: SQL Client Stream Collect Sink (1/1) (97aad4ff05ca3033f5ef08084ee4abc9) switched from CREATED to SCHEDULED.
2020-04-18 11:58:50,544 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl      - Cannot serve slot request, no ResourceManager connected. Adding as pending request [SlotRequestId{3ca529d11130e8e5e99fb8e4632c4892}]
2020-04-18 11:58:50,552 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Connecting to ResourceManager akka.tcp://flink@localhost:6123/user/resourcemanager(00000000000000000000000000000000)
2020-04-18 11:58:50,555 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Resolved ResourceManager address, beginning registration
2020-04-18 11:58:50,555 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - Registration at ResourceManager attempt 1 (timeout=100ms)
2020-04-18 11:58:50,557 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager  - Registering job manager 00000000000000000000000000000000@akka.tcp://flink@localhost:6123/user/jobmanager_0 for job eb9e2c44fdcd2f6719188f515ddad7ae.
2020-04-18 11:58:50,562 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager  - Registered job manager 00000000000000000000000000000000@akka.tcp://flink@localhost:6123/user/jobmanager_0 for job eb9e2c44fdcd2f6719188f515ddad7ae.
2020-04-18 11:58:50,563 INFO  org.apache.flink.runtime.jobmaster.JobMaster                  - JobManager successfully registered at ResourceManager, leader id: 00000000000000000000000000000000.
2020-04-18 11:58:50,564 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl      - Requesting new slot [SlotRequestId{3ca529d11130e8e5e99fb8e4632c4892}] and profile ResourceProfile{UNKNOWN} from resource manager.
2020-04-18 11:58:50,565 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager  - Request slot with profile ResourceProfile{UNKNOWN} for job eb9e2c44fdcd2f6719188f515ddad7ae with allocation id 078993c8cd47de2cbf09fa5387b73df7.
2020-04-18 11:58:50,645 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: KafkaTableSource(user_id, item_id, category_id, behavior, ts) -> SourceConversion(table=[default_catalog.default_database.user_behavior, source: [KafkaTableSource(user_id, item_id, category_id, behavior, ts)]], fields=[user_id, item_id, category_id, behavior, ts]) -> Calc(select=[user_id, item_id, category_id, behavior, ts, () AS proctime]) -> WatermarkAssigner(rowtime=[ts], watermark=[(ts - 5000:INTERVAL SECOND)]) -> Calc(select=[ts], where=[(behavior = _UTF-16LE'buy':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")]) (1/1) (dab698f7e11df09032d07bec79d8dffd) switched from SCHEDULED to DEPLOYING.
2020-04-18 11:58:50,645 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Deploying Source: KafkaTableSource(user_id, item_id, category_id, behavior, ts) -> SourceConversion(table=[default_catalog.default_database.user_behavior, source: [KafkaTableSource(user_id, item_id, category_id, behavior, ts)]], fields=[user_id, item_id, category_id, behavior, ts]) -> Calc(select=[user_id, item_id, category_id, behavior, ts, () AS proctime]) -> WatermarkAssigner(rowtime=[ts], watermark=[(ts - 5000:INTERVAL SECOND)]) -> Calc(select=[ts], where=[(behavior = _UTF-16LE'buy':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")]) (1/1) (attempt #0) to ddf441898e05993ca96163409ecde9b3 @ localhost (dataPort=49815)
2020-04-18 11:58:50,649 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - GroupWindowAggregate(window=[TumblingGroupWindow('w$, ts, 3600000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[COUNT(*) AS EXPR$1, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime]) -> Calc(select=[(FLAG(HOUR) EXTRACT w$start) AS EXPR$0, EXPR$1]) -> SinkConversionToTuple2 -> Sink: SQL Client Stream Collect Sink (1/1) (97aad4ff05ca3033f5ef08084ee4abc9) switched from SCHEDULED to DEPLOYING.
2020-04-18 11:58:50,649 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Deploying GroupWindowAggregate(window=[TumblingGroupWindow('w$, ts, 3600000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[COUNT(*) AS EXPR$1, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime]) -> Calc(select=[(FLAG(HOUR) EXTRACT w$start) AS EXPR$0, EXPR$1]) -> SinkConversionToTuple2 -> Sink: SQL Client Stream Collect Sink (1/1) (attempt #0) to ddf441898e05993ca96163409ecde9b3 @ localhost (dataPort=49815)
2020-04-18 11:58:50,784 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - GroupWindowAggregate(window=[TumblingGroupWindow('w$, ts, 3600000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[COUNT(*) AS EXPR$1, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime]) -> Calc(select=[(FLAG(HOUR) EXTRACT w$start) AS EXPR$0, EXPR$1]) -> SinkConversionToTuple2 -> Sink: SQL Client Stream Collect Sink (1/1) (97aad4ff05ca3033f5ef08084ee4abc9) switched from DEPLOYING to RUNNING.
2020-04-18 11:58:50,785 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: KafkaTableSource(user_id, item_id, category_id, behavior, ts) -> SourceConversion(table=[default_catalog.default_database.user_behavior, source: [KafkaTableSource(user_id, item_id, category_id, behavior, ts)]], fields=[user_id, item_id, category_id, behavior, ts]) -> Calc(select=[user_id, item_id, category_id, behavior, ts, () AS proctime]) -> WatermarkAssigner(rowtime=[ts], watermark=[(ts - 5000:INTERVAL SECOND)]) -> Calc(select=[ts], where=[(behavior = _UTF-16LE'buy':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")]) (1/1) (dab698f7e11df09032d07bec79d8dffd) switched from DEPLOYING to RUNNING.

