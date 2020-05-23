---
layout:     post
title:      Flink源码阅读之Watermark对齐
subtitle:   Flink源码阅读之Watermark对齐
date:       2020-05-23
author:     Sun.Zhu
header-img: img/210540-1576933540edc3.jpg
catalog: true
tags:
    - Flink
    - watermark
---

## watermark的产生
我们知道watermark的生成有两种方式：
1.在sourceFunction中通过emitWatermark方法生成
2.通过assignTimestampsAndWatermarks抽取timestamp并生成watermark
## watermark的流转
watermark会像普通的element和stream status一样随着stream流不断的向下游流转
## watermark的处理
前面文章写过[job的执行过程](https://www.jianshu.com/p/3b9b4b01134f)，描述了数据怎么被处理的，所有流数据被封装成StreamElement对象，其有4个子类
![image.png](https://upload-images.jianshu.io/upload_images/22945753-9184c8f5cb4f7713.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
本文主要讨论waterm对齐的过程
具体处理逻辑在StreamTaskNetworkInput#processElement方法中
```
private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {
                        //处理watermark
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
```
public void inputWatermark(Watermark watermark, int channelIndex) throws Exception {
		// ignore the input watermark if its input channel, or all input channels are idle (i.e. overall the valve is idle).
        //当前流状态是active，input channel的状态也是active，否则不处理
		if (lastOutputStreamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isActive()) {
			long watermarkMillis = watermark.getTimestamp();

			// if the input watermark's value is less than the last received watermark for its input channel, ignore it also.
            //如果当前水印时间小于等于当前channel的水印时间，则忽略
			if (watermarkMillis > channelStatuses[channelIndex].watermark) {
				channelStatuses[channelIndex].watermark = watermarkMillis;

				// previously unaligned input channels are now aligned if its watermark has caught up
                //如果当前channel是未对齐状态，且当前水印时间大于上次发出的水印时间则任务当前channel已经对齐
				if (!channelStatuses[channelIndex].isWatermarkAligned && watermarkMillis >= lastOutputWatermark) {
					channelStatuses[channelIndex].isWatermarkAligned = true;
				}

				// now, attempt to find a new min watermark across all aligned channels
                //取所有input channel中最小的watermark，当做最新的watermark发出  
				findAndOutputNewMinWatermarkAcrossAlignedChannels();
			}
		}
	}
```
flink通过InputChannelStatus数组来维护一个算子的所有input channel
```
/**
	 * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
	 * status, and whether or not the channel's current watermark is aligned with the overall
	 * watermark output from the valve.
	 *
	 * <p>There are 2 situations where a channel's watermark is not considered aligned:
	 * <ul>
	 *   <li>the current stream status of the channel is idle
	 *   <li>the stream status has resumed to be active, but the watermark of the channel hasn't
	 *   caught up to the last output watermark from the valve yet.
	 * </ul>
	 */
	@VisibleForTesting
	protected static class InputChannelStatus {
		protected long watermark;//当前channel最后一个watermark的时间戳
		protected StreamStatus streamStatus;//流的状态
		protected boolean isWatermarkAligned;//watermark是否对齐
}
```
上面的doc描述的很清楚，一个InputChannelStatus对象维护一个channel发送的最后一个watermark的时间戳，当前流的状态（active or idle），当前watermark是否和总体输出的watermark对齐。
同时有2种情况下不需要对齐：
1.当前stream状态是idle。
2.stream状态是刚恢复成active，且当前channel的watermark还没赶上总体输出的最新的水印。

上面inputWatermark方法的注释已经大概说明了channel对齐的过程。取所有input channel的最小watermark并作为当前watermark逻辑如下：
```
private void findAndOutputNewMinWatermarkAcrossAlignedChannels() throws Exception {
		long newMinWatermark = Long.MAX_VALUE;
		boolean hasAlignedChannels = false;

		// determine new overall watermark by considering only watermark-aligned channels across all channels
        //只有已对齐的channel才会参与比较
		for (InputChannelStatus channelStatus : channelStatuses) {
			if (channelStatus.isWatermarkAligned) {
				hasAlignedChannels = true;
				newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
			}
		}

		// we acknowledge and output the new overall watermark if it really is aggregated
		// from some remaining aligned channel, and is also larger than the last output watermark
        //从已对其的channel中获取到的最小的watermark，如果大于上次发出的watermark则作为最新的watermark发出。
		if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
			lastOutputWatermark = newMinWatermark;
			output.emitWatermark(new Watermark(lastOutputWatermark));
		}
	}
```
至此watermark对齐取最小的逻辑已分析完毕。
