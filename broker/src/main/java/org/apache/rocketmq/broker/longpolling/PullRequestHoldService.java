/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

/**
 * 该组件的职责：挂起无法被立即满足的消息拉取请求
 * 处理流程：周期性检查所有topic@queue的拉取请求能够被满足或者是否超时，然后将符合条件的拉取请求交给拉取处理器处理
 */
public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    // 存储针对topic的queue的多条拉取消息请求
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 挂起消息拉取请求
     *
     * @param topic       当前拉取请求针对的topic名称
     * @param queueId     当前拉取请求针对的topic下的queue
     * @param pullRequest 需要挂起的拉取请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        // 基于topic@queueid存储PullRequest实例，将多个同一topic下同一队列中的请求放入同一容器中管理
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                // 确认是否启用长轮询，启用后将请求挂起一段时间，默认启用，每个等待周期为5秒钟
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                // 检查内部的挂起请求
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        // 为什么需要更好地对业务组件进行命名，可以直接利用类名来作为线程名
        return PullRequestHoldService.class.getSimpleName();
    }

    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                // 针对每个被挂起的请求，获取请求的topic@queue下当前最大的offset
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    /**
     * 通知消息到达
     *
     * @param topic     topic名称
     * @param queueId   队列名
     * @param maxOffset 当前队列中消息的最大offset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic,
                                      final int queueId,
                                      final long maxOffset,
                                      final Long tagsCode,
                                      long msgStoreTime,
                                      byte[] filterBitMap,
                                      Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        // 设计精妙之处一：将所有针对同一topic@queue的消息拉取请求存储在一起，这样请求暂存组件可以检查queue中的最新消息后一次性唤醒所有
        // 针对当前队列的请求
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    // 之前计算的Broker中的最大的offset比当前PullRequest要拉取的offset都小，
                    // 说明之前的offset太落后了，需要重新拉取目标queue最新的offset
                    // 大白话就是Consumer要拉取的消息比内部发现的新消息的Offset要大，之前读取到的最大的offset并不能满足当前的挂起请求
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        // 这里重新拉取当前topic@queue中最大的消息offset
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    // 当topic@queue中的最大消息offset能够满足本次拉取请求时，开始尝试处理请求
                    if (newestOffset > request.getPullFromThisOffset()) {
                        // 根据消息过滤器确认消息是否匹配拉取请求中携带的标签要求
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                                new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            // 对请求中包含筛选属性时需要再次对消息内容进行筛选
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                // 最新消息能够满足拉取请求时，交给拉取请求处理器执行
                                // 这里可以看出RocketMQ中的组件设计精妙之处，挂起请求组件仅仅负责检查每个请求想要拉取的消息能否被满足
                                // 具体的消息拉取请求的处理仍交给对应的组件处理，提高了组件的内聚性
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(
                                        request.getClientChannel(),
                                        request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }

                    // TODO 这里个人感觉可以将判断消息拉取请求已经超时的逻辑封装在request内部，使得语义更加明确
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            // 当消息拉取请求已经超时的情况下也需要立刻处理
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    // 既无法满足消息拉取，也没有超时的请求需要继续进行挂起
                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    // 重新挂起请求
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
