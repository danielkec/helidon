/*
 * Copyright (c) 2020 Oracle and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.helidon.messaging.connectors.rsocket;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.helidon.common.configurable.ScheduledThreadPoolSupplier;
import io.helidon.config.Config;
import io.helidon.config.mp.MpConfig;
import io.helidon.messaging.Stoppable;

import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscriber;

/**
 * Implementation of RSocket Connector as described in the MicroProfile Reactive Messaging Specification.
 */
@ApplicationScoped
@Connector(RSocketConnector.CONNECTOR_NAME)
public class RSocketConnector implements IncomingConnectorFactory, OutgoingConnectorFactory, Stoppable {

    private static final Logger LOGGER = Logger.getLogger(RSocketConnector.class.getName());
    /**
     * Microprofile messaging RSocket connector name.
     */
    static final String CONNECTOR_NAME = "helidon-rsocket";

    ConcurrentHashMap<String, Subscriber<Message<?>>> subscriberMap = new ConcurrentHashMap<>();

    /**
     * Constructor to instance RSocketConnectorFactory.
     *
     * @param config Helidon {@link io.helidon.config.Config config}
     */
    @Inject
    RSocketConnector(Config config) {
    }

    /**
     * Called when container is terminated. If it is not running in a container it must be explicitly invoked
     * to terminate the messaging and release RSocket connections.
     *
     * @param event termination event
     */
    void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
        stop();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(org.eclipse.microprofile.config.Config config) {
        return null;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(org.eclipse.microprofile.config.Config config) {
        String channelName = config.getValue(ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, String.class);
        return ReactiveStreams.fromSubscriber(subscriberMap.get(channelName));
    }

    /**
     * Creates a new instance of RSocketConnector with the required configuration.
     *
     * @param config Helidon {@link io.helidon.config.Config config}
     * @return the new instance
     */
    public static RSocketConnector create(Config config) {
        return new RSocketConnector(config);
    }

    /**
     * Creates a new instance of RSocketConnector with empty configuration.
     *
     * @return the new instance
     */
    public static RSocketConnector create() {
        return new RSocketConnector(Config.empty());
    }

    /**
     * Stops the RSocketConnector and all the jobs and resources related to it.
     */
    public void stop() {
        LOGGER.fine(() -> "Terminating RSocketConnector...");

    }
}

