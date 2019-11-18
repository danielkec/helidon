/*
 * Copyright (c)  2019 Oracle and/or its affiliates. All rights reserved.
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

package io.helidon.microprofile.messaging.reactive;

import io.helidon.microprofile.messaging.MessageUtils;
import io.helidon.microprofile.messaging.channel.ProcessorMethod;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

/**
 * Process every item in stream by method ex:
 * <pre>{@code
 *      @Incoming("inner-processor")
 *      @Outgoing("inner-consumer")
 *      public String process2(String msg) {
 *          return msg.toLowerCase();
 *      }
 * }</pre>
 */
public class InternalProcessor implements Processor<Object, Object> {


    private ProcessorMethod processorMethod;
    private Subscriber<? super Object> subscriber;

    public InternalProcessor(ProcessorMethod processorMethod) {
        this.processorMethod = processorMethod;
    }

    @Override
    public void subscribe(Subscriber<? super Object> s) {
        subscriber = s;
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscriber.onSubscribe(s);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onNext(Object incomingValue) {
        try {
            Method method = processorMethod.getMethod();
            //TODO: Has to be always one param in the processor, validate and propagate better
            Class<?> paramType = method.getParameterTypes()[0];
            Object processedValue = method.invoke(processorMethod.getBeanInstance(),
                    MessageUtils.unwrap(incomingValue, paramType));
            //TODO: Extract as some sensible util
            if (processedValue instanceof Publisher || processedValue instanceof PublisherBuilder) {
                //Flatten, we are sure its invoke on every request method now
                PublisherBuilder<Object> publisherBuilder = null;
                if (processedValue instanceof Publisher) {
                    publisherBuilder = ReactiveStreams.fromPublisher((Publisher<Object>) processedValue);
                } else {
                    publisherBuilder = (PublisherBuilder<Object>) processedValue;
                }
                publisherBuilder.forEach(subVal -> {
                    try {
                        subscriber.onNext(wrapValue(subVal));
                    } catch (ExecutionException | InterruptedException e) {
                        subscriber.onError(e);
                    }
                }).run();
            } else {
                subscriber.onNext(wrapValue(processedValue));
            }
        } catch (IllegalAccessException | InvocationTargetException | ExecutionException | InterruptedException e) {
            subscriber.onError(e);
        }
    }

    protected Object wrapValue(Object value) throws ExecutionException, InterruptedException {
        return MessageUtils.unwrap(value, Message.class);
    }

    @Override
    public void onError(Throwable t) {
        subscriber.onError(t);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }
}
