/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.gemfire;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.Lifecycle;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

/**
 * {@link MessageHandler} implementation that publishes messages
 * to a {@link Region}.
 */
public class SendingHandler implements MessageHandler, Lifecycle {
	private static final Logger logger = LoggerFactory.getLogger(SendingHandler.class);

	private final String name;

	private final Cache cache;

	private final AtomicLong sequence = new AtomicLong();

	private final int pid;

	private final long timestamp = System.currentTimeMillis();

	private volatile boolean running;

	private final Region<String, Set<String>> consumerGroupsRegion;

	private final Map<String, Region<MessageKey, Message<?>>> producerRegionMap = new ConcurrentHashMap<>();

	public SendingHandler(Cache cache, Region<String, Set<String>> consumerGroupsRegion, String name) {
		this.cache = cache;
		this.name = name;
		this.consumerGroupsRegion = consumerGroupsRegion;
		this.pid = cache.getDistributedSystem().getDistributedMember().getProcessId();
	}

	/**
	 * Create a {@link Region} instance used for publishing {@link Message} objects.
	 * This region instance will not store buckets; it is assumed that the regions
	 * created by consumers will host buckets.
	 *
	 * @param name name of the message region
	 * @return region for producing messages
	 */
	private Region<MessageKey, Message<?>> createProducerMessageRegion(String name) {
		RegionFactory<MessageKey, Message<?>> factory = this.cache.createRegionFactory(RegionShortcut.PARTITION_PROXY);
		return factory.addAsyncEventQueueId(name + GemfireMessageChannelBinder.QUEUE_POSTFIX)
				.create(name + GemfireMessageChannelBinder.MESSAGES_POSTFIX);
	}

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		logger.trace("Publishing message {}", message);
		handleRemovedConsumerGroups();
		Set<String> groups = this.consumerGroupsRegion.get(this.name);
		for (String group : groups) {
			String regionName = GemfireMessageChannelBinder.formatMessageRegionName(this.name, group);
			Region<MessageKey, Message<?>> region = this.producerRegionMap.get(regionName);
			if (region == null) {
				region = createProducerMessageRegion(regionName);
				this.producerRegionMap.put(regionName, region);
			}
			region.putAll(Collections.singletonMap(nextMessageKey(), message));
		}
	}

	private void handleRemovedConsumerGroups() {
		Set<String> registeredGroups = this.consumerGroupsRegion.get(this.name);
		Set<String> knownGroups = this.producerRegionMap.keySet();
		Set<String> removedGroups = new HashSet<>(knownGroups);
		removedGroups.removeAll(registeredGroups);
		for (String group : removedGroups) {
			this.producerRegionMap.remove(group).close();
		}
	}

	private MessageKey nextMessageKey() {
		return new MessageKey(sequence.getAndIncrement(), timestamp, pid);
	}

	@Override
	public void start() {
		this.running = true;
	}

	@Override
	public void stop() {
		this.running = false;
		for (Region<MessageKey, Message<?>> region : this.producerRegionMap.values()) {
			region.close();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}
}
