/*
 * Copyright (c)  2020 Oracle and/or its affiliates.
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
 *
 */

package io.helidon.messaging;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import io.helidon.common.configurable.ThreadPoolSupplier;
import io.helidon.common.reactive.Multi;
import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import io.helidon.config.ConfigValue;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.reactivestreams.Publisher;

class MessagingImpl implements Messaging {

    private final Set<Emitter<?>> emitters = new HashSet<>();
    private final Map<String, Channel<?>> channelMap = new HashMap<>();
    private final Map<String, IncomingConnectorFactory> incomingConnectors = new HashMap<>();
    private final Map<String, OutgoingConnectorFactory> outgoingConnectors = new HashMap<>();
    private Config config;
    private MessagingImpl.State state = MessagingImpl.State.INIT;
    private ThreadPoolSupplier threadPoolSupplier;

    MessagingImpl() {
    }

    public void start() {
        state.start(this);
        threadPoolSupplier = ThreadPoolSupplier.builder()
                .threadNamePrefix("helidon-messaging-")
                .build();
        emitters.forEach(emitter -> emitter.init(threadPoolSupplier.get(), Flow.defaultBufferSize()));
        channelMap.values().forEach(this::findConnectors);
        channelMap.values().forEach(Channel::connect);
    }

    public void stop() {
        state.stop(this);
        Multi.concat(
                Multi.from(incomingConnectors.values()).map(Object.class::cast),
                Multi.from(outgoingConnectors.values()).map(Object.class::cast))
                .distinct()
                .filter(Stoppable.class::isInstance)
                .map(Stoppable.class::cast)
                .forEach(Stoppable::stop);
        emitters.forEach(Emitter::complete);
        threadPoolSupplier.get().shutdown();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Emitter<T> emitter(String channel) {
        Publisher<?> publisher = channelMap.get(channel).getPublisher();
        if (publisher instanceof Emitter) {
            return (Emitter<T>) publisher;
        }
        throw new MessagingException("Channel " + channel + " doesn't have emitter as publisher!");
    }

    @Override
    public <T> CompletionStage<Void> send(final String channel, final T msg) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.emitter(channel).send(Message.of(msg, () -> {
            future.complete(null);
            return CompletableFuture.completedStage(null);
        }));
        return future;
    }

    @Override
    public <T, M extends Message<T>> void send(final String channel, final M msg) {
        this.emitter(channel).send(msg);
    }

    void setConfig(final Config config) {
        this.config = config;
    }

    void addIncomingConnector(IncomingConnectorFactory connector) {
        incomingConnectors.put(getConnectorName(connector.getClass()), connector);
    }

    void addOutgoingConnector(OutgoingConnectorFactory connector) {
        outgoingConnectors.put(getConnectorName(connector.getClass()), connector);
    }

    void addEmitter(Emitter<?> emitter) {
        emitters.add(emitter);
    }

    void registerChannel(Channel<?> channel) {
        Channel<?> ch = channelMap.get(channel.name());
        if (ch == null) {
            ch = channel;
            channelMap.put(ch.name(), ch);
        }
    }

    private String getConnectorName(Class<?> clazz) {
        Connector annotation = clazz.getAnnotation(Connector.class);
        if (annotation == null) {
            throw new MessagingException("Missing @Connector annotation in provided " + clazz.getSimpleName());
        }
        return annotation.value();
    }

    private void findConnectors(Channel<?> channel) {
        Config.Builder configBuilder = Config.builder()
                .disableSystemPropertiesSource()
                .disableEnvironmentVariablesSource();
        if (config != null) {
            configBuilder.addSource(ConfigSources.create(config));
        }

        if (channel.publisherConfig != null) {
            configBuilder
                    .addSource(ConnectorConfigHelper
                            .prefixedConfigSource(ConnectorFactory.OUTGOING_PREFIX + channel.name(),
                                    channel.publisherConfig));
        }

        if (channel.subscriberConfig != null) {
            configBuilder
                    .addSource(ConnectorConfigHelper
                            .prefixedConfigSource(ConnectorFactory.INCOMING_PREFIX + channel.name(),
                                    channel.subscriberConfig));
        }
        Config mergedConfig = configBuilder.build();

        ConfigValue<String> incomingConnectorName = ConnectorConfigHelper.getIncomingConnectorName(mergedConfig, channel.name());
        ConfigValue<String> outgoingConnectorName = ConnectorConfigHelper.getOutgoingConnectorName(mergedConfig, channel.name());

        if (incomingConnectorName.isPresent()) {
            String connectorName = incomingConnectorName.get();
            org.eclipse.microprofile.config.Config incomingConnectorConfig =
                    ConfigurableConnector.getConnectorConfig(channel.name(), connectorName, mergedConfig);
            channel.setPublisher(
                    incomingConnectors.get(connectorName)
                            .getPublisherBuilder(incomingConnectorConfig)
                            .buildRs());
        }
        if (outgoingConnectorName.isPresent()) {
            String connectorName = outgoingConnectorName.get();
            org.eclipse.microprofile.config.Config outgoingConnectorConfig =
                    ConfigurableConnector.getConnectorConfig(channel.name(), connectorName, mergedConfig);
            channel.setSubscriber(
                    outgoingConnectors.get(connectorName)
                            .getSubscriberBuilder(outgoingConnectorConfig)
                            .build());
        }
    }

    enum State {
        INIT {
            void start(MessagingImpl m) {
                m.state = STARTED;
            }

            void stop(MessagingImpl m) {
                throw new MessagingException("Messaging is not started yet!");
            }
        },
        STARTED {
            void start(MessagingImpl m) {
                throw new MessagingException("Messaging has been started already!");
            }

            void stop(MessagingImpl m) {
                m.state = STOPPED;
            }
        },
        STOPPED {
            void start(MessagingImpl m) {
                throw new MessagingException("Messaging has been stopped already!");
            }

            void stop(MessagingImpl m) {
                start(m);
            }
        };

        abstract void start(MessagingImpl m);

        abstract void stop(MessagingImpl m);
    }
}
