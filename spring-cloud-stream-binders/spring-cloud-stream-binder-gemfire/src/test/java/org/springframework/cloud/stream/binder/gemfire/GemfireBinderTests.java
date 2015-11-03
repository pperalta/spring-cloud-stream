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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.distributed.LocatorLauncher;
import com.oracle.tools.runtime.LocalPlatform;
import com.oracle.tools.runtime.PropertiesBuilder;
import com.oracle.tools.runtime.concurrent.RemoteCallable;
import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.LocalJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.GenericMessage;

/**
 * Tests for {@link GemfireMessageChannelBinder}.
 *
 * @author Patrick Peralta
 */
public class GemfireBinderTests {
	private static final Logger logger = LoggerFactory.getLogger(GemfireBinderTests.class);

	/**
	 * Timeout value in milliseconds for operations to complete.
	 */
	private static final long TIMEOUT = 30000;

	/**
	 * Port number for Gemfire locator.
	 */
	private static final int LOCATOR_PORT = 7777;

	/**
	 * Payload of test message.
	 */
	private static final String MESSAGE_PAYLOAD = "hello world";

	/**
	 * Name of binding used for producer and consumer bindings.
	 */
	private static final String BINDING_NAME = "test";

	/**
	 * Test basic message sending functionality.
	 *
	 * @throws Exception
	 */
	@Test
	public void testMessageSendReceive() throws Exception {
		LocatorLauncher locatorLauncher = null;
		JavaApplication consumer = null;
		JavaApplication producer = null;

		try {
			locatorLauncher = new LocatorLauncher.Builder()
					.setMemberName("locator1")
					.setPort(LOCATOR_PORT)
					.setRedirectOutput(true)
					.setWorkingDirectory(System.getProperty("java.io.tmpdir"))
					.build();

			locatorLauncher.start();
			locatorLauncher.waitOnStatusResponse(TIMEOUT, 5, TimeUnit.MILLISECONDS);

			consumer = launch(Consumer.class, null, null);
			waitForConsumer(consumer);

			producer = launch(Producer.class, null, null);
			assertEquals(MESSAGE_PAYLOAD, waitForMessage(consumer));
		}
 		finally {
			if (producer != null) {
				producer.close();
			}
			if (consumer != null) {
				consumer.close();
			}
			if (locatorLauncher != null) {
				locatorLauncher.stop();
			}
		}
	}

	/**
	 * Block the executing thread until the consumer is bound.
	 *
	 * @param consumer the consumer application
	 * @throws InterruptedException if the thread is interrupted
	 * @throws AssertionError if the consumer is not bound after
	 * {@value #TIMEOUT} milliseconds
	 */
	private void waitForConsumer(JavaApplication consumer) throws InterruptedException {
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() < start + TIMEOUT) {
			if (consumer.submit(new ConsumerBoundChecker())) {
				return;
			}
			else {
				Thread.sleep(1000);
			}
		}
		assertTrue("Consumer not bound", consumer.submit(new ConsumerBoundChecker()));
	}

	/**
	 * Block the executing thread until a message is received by the
	 * consumer application, or until {@value #TIMEOUT} milliseconds elapses.
	 *
	 * @param consumer the consumer application
	 * @return the message payload that was received
	 * @throws InterruptedException if the thread is intterrupted
	 */
	private String waitForMessage(JavaApplication consumer) throws InterruptedException {
		long start = System.currentTimeMillis();
		String message = null;
		while (System.currentTimeMillis() < start + TIMEOUT) {
			message = consumer.submit(new ConsumerMessageExtractor());
			if (message == null) {
				Thread.sleep(1000);
			}
			else {
				break;
			}
		}
		return message;
	}

	/**
	 * Launch the given class's {@code main} method in a separate JVM.
	 *
	 * @param clz               class to launch
	 * @param systemProperties  system properties for new process
	 * @param args              command line arguments
	 * @return launched application
	 *
	 * @throws IOException if an exception was thrown launching the process
	 */
	private JavaApplication launch(Class<?> clz, Properties systemProperties,
			List<String> args) throws IOException {
		String classpath = System.getProperty("java.class.path");

		logger.info("Launching {}", clz);
		logger.info("	args: {}", args);
		logger.info("	properties: {}", systemProperties);
		logger.info("	classpath: {}", classpath);

		SimpleJavaApplicationSchema schema = new SimpleJavaApplicationSchema(clz.getName(), classpath);
		if (args != null) {
			for (String arg : args) {
				schema.addArgument(arg);
			}
		}
		if (systemProperties != null) {
			schema.setSystemProperties(new PropertiesBuilder(systemProperties));
		}

		LocalJavaApplicationBuilder<SimpleJavaApplication> builder =
				new LocalJavaApplicationBuilder<>(LocalPlatform.getInstance());
		return builder.realize(schema, clz.getName(), new SystemApplicationConsole());
	}

	/**
	 * Producer application that binds a channel to a {@link GemfireMessageChannelBinder}
	 * and sends a test message.
	 */
	public static class Producer {
		public static void main(String[] args) throws Exception {
			GemfireMessageChannelBinder binder = new GemfireMessageChannelBinder();
			binder.setApplicationContext(new GenericApplicationContext());
			binder.afterPropertiesSet();

			SubscribableChannel producerChannel = new ExecutorSubscribableChannel();

			binder.bindProducer(BINDING_NAME, producerChannel, new Properties());

			Message<String> message = new GenericMessage<>(MESSAGE_PAYLOAD);
			producerChannel.send(message);

			Thread.sleep(1000);
		}
	}

	/**
	 * Consumer application that binds a channel to a {@link GemfireMessageChannelBinder}
	 * and stores the received message payload.
	 */
	public static class Consumer {

		/**
		 * Flag that indicates if the consumer has been bound.
		 */
		private static volatile boolean isBound = false;

		/**
		 * Payload of last received message.
		 */
		private static volatile String messagePayload;

		public static void main(String[] args) throws Exception {
			GemfireMessageChannelBinder binder = new GemfireMessageChannelBinder();
			binder.setApplicationContext(new GenericApplicationContext());
			binder.afterPropertiesSet();

			SubscribableChannel consumerChannel = new ExecutorSubscribableChannel();
			consumerChannel.subscribe(new MessageHandler() {
				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					String payload = (String) message.getPayload();
					logger.debug("received message " + payload);
					messagePayload = payload;
				}
			});
			binder.bindConsumer(BINDING_NAME, consumerChannel, new Properties());
			isBound = true;

			Thread.sleep(Long.MAX_VALUE);
		}
	}

	public static class ConsumerBoundChecker implements RemoteCallable<Boolean> {
		@Override
		public Boolean call() throws Exception {
			return Consumer.isBound;
		}
	}

	public static class ConsumerMessageExtractor implements RemoteCallable<String> {
		@Override
		public String call() throws Exception {
			return Consumer.messagePayload;
		}
	}

}
