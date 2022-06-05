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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 当前broker节点作为slave节点时
 * 由本组件负责从master节点同步核心数据到本地，具体如下：
 * 1.topic元数据
 * 2.consumer消费元数据
 * 3.延迟消息消费元数据
 * 4.订阅组元数据
 */
public class SlaveSynchronize {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private volatile String masterAddr = null;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    public void syncAll() {
        // 同步topic配置信息
        this.syncTopicConfig();
        // 同步consumer的消费offset信息
        this.syncConsumerOffset();
        // 同步consumer的delay消息offset
        this.syncDelayOffset();
        // 同步consumer订阅配置信息
        this.syncSubscriptionGroupConfig();
    }

    /**
     * 向Master节点同步所有topic元数据
     */
    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        // 当前节点属于slave节点
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 向Master节点获取所有topic元数据进行数据版本对比
                TopicConfigSerializeWrapper topicWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(topicWrapper.getDataVersion())) {
                    // 发现本地版本与Master节点版本不一致的处理：
                    // 清空本地topic元数据，并将Master节点的元数据保存到本地
                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(topicWrapper.getTopicConfigTable());
                    this.brokerController.getTopicConfigManager().persist();

                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 将master节点的消费进度同步到本地
                ConsumerOffsetSerializeWrapper offsetWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(offsetWrapper.getOffsetTable());
                // 持久化到文件
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 将延迟消息的消费偏移量同步到本地文件
                String delayOffset =
                    this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    String fileName =
                        StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                            .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null  && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                SubscriptionGroupWrapper subscriptionWrapper =
                    this.brokerController.getBrokerOuterAPI()
                        .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                    .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager =
                        this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                        subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                        subscriptionWrapper.getSubscriptionGroupTable());
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}
