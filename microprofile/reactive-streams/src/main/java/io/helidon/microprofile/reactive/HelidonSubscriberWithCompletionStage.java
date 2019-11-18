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

package io.helidon.microprofile.reactive;

import io.helidon.common.reactive.Flow;
import io.helidon.microprofile.reactive.hybrid.HybridProcessor;
import org.eclipse.microprofile.reactive.streams.operators.spi.Stage;
import org.eclipse.microprofile.reactive.streams.operators.spi.SubscriberWithCompletionStage;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class HelidonSubscriberWithCompletionStage<T> implements SubscriberWithCompletionStage<T, Object> {

    private final Processor<Object, Object> connectingProcessor;
    private Subscriber<Object> subscriber;
    private CompletableFuture<Object> completableFuture = new CompletableFuture<>();
    private Stage.Collect collectStage;
    private LinkedList<Processor<Object, Object>> processorList = new LinkedList<>();


    /**
     * Subscriber with preceding processors included, automatically makes all downstream subscriptions when its subscribe method is called.
     *
     * @param collectStage
     * @param precedingProcessorList
     */
    public HelidonSubscriberWithCompletionStage(Stage.Collect collectStage, List<Flow.Processor<Object, Object>> precedingProcessorList) {
        this.collectStage = collectStage;
        //preceding processors
        precedingProcessorList.forEach(fp -> this.processorList.add(HybridProcessor.from(fp)));
        subscriber = (Subscriber<Object>) prepareSubscriber();
        connectingProcessor = prepareConnectingProcessor();
    }

    @Override
    public CompletionStage<Object> getCompletion() {
        return completableFuture;
    }

    @Override
    public Subscriber<T> getSubscriber() {
        return (Subscriber<T>) connectingProcessor;
    }

    private Subscriber<T> prepareSubscriber() {
        return new Subscriber<T>() {

            private Subscription subscription;
            private final AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                subscription.request(1);
            }

            @Override
            public void onNext(Object t) {
                if (!closed.get()) {
                    BiConsumer<Object, Object> accumulator = (BiConsumer) collectStage.getCollector().accumulator();
                    accumulator.accept(null, t);
                    subscription.request(1);
                }
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException(t);
            }

            @Override
            public void onComplete() {
                closed.set(true);
                completableFuture.complete(null);
                subscription.cancel();
            }
        };
    }

    private Processor<Object, Object> prepareConnectingProcessor() {
        return new Processor<Object, Object>() {
            @Override
            public void subscribe(Subscriber<? super Object> s) {
                processorList.getFirst().subscribe(s);
            }

            @Override
            public void onSubscribe(Subscription s) {
                // This is a time for connecting all pre-processors and subscriber
                Processor<Object, Object> lastProcessor = null;
                for (Iterator<Processor<Object, Object>> it = processorList.iterator(); it.hasNext(); ) {
                    Processor<Object, Object> processor = it.next();
                    if (lastProcessor != null) {
                        lastProcessor.subscribe(processor);
                    }
                    lastProcessor = processor;
                }
                if (!processorList.isEmpty()) {
                    processorList.getLast().subscribe(subscriber);
                    // First preprocessor act as subscriber
                    subscriber = processorList.getFirst();
                }
                //No processors just forward to subscriber
                subscriber.onSubscribe(s);
            }

            @Override
            public void onNext(Object o) {
                subscriber.onNext(o);
            }

            @Override
            public void onError(Throwable t) {
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        };
    }
}