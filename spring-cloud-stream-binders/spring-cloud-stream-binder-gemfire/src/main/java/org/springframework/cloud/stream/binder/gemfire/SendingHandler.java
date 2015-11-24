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
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.RegionMembershipListenerAdapter;
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

	private final Region<MessageKey, Message<?>> messageRegion;

	private final AtomicLong sequence = new AtomicLong();

	private final int pid;

	private final long timestamp = System.currentTimeMillis();

	private volatile boolean running;

	public SendingHandler(Cache cache, String name) {
		this.cache = cache;
		this.name = name;
		this.pid = cache.getDistributedSystem().getDistributedMember().getProcessId();
	}

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		logger.trace("Publishing message {}", message);
		this.messageRegion.putAll(Collections.singletonMap(nextMessageKey(), message));
	}

	private MessageKey nextMessageKey() {
		return new MessageKey(sequence.getAndIncrement(), timestamp, pid);
	}

	@Override
	public synchronized void start() {
		if (!running) {

		}


	}

	@Override
	public void stop() {

	}

	@Override
	public boolean isRunning() {
		return false;
	}

	static class RegionLifecycleListener extends RegionMembershipListenerAdapter<String, Message<?>> {

	}
}
