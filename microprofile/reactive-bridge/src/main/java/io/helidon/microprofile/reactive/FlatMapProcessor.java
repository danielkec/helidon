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

import io.helidon.common.reactive.BaseProcessor;
import io.helidon.common.reactive.Flow;
import io.helidon.common.reactive.Multi;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.spi.Graph;
import org.reactivestreams.Publisher;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class FlatMapProcessor extends BaseProcessor<Object, Object> implements Multi<Object> {

    private final Function<Object, Graph> mapper;
    private Flow.Subscriber<? super Object> subscriber;

    public FlatMapProcessor(Function<?, Graph> mapper) {
        this.mapper = (Function<Object, Graph>) mapper;
    }

    @Override
    protected void hookOnNext(Object item) {
        Graph graph = mapper.apply(item);
        HelidonReactiveStreamEngine streamEngine = new HelidonReactiveStreamEngine();
        Publisher<Object> publisher = streamEngine.buildPublisher(graph);
        try {
            ReactiveStreams
                    .fromPublisher(publisher)
                    .forEach(subItem -> submit(subItem))
                    .run().toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            onError(e);
        }
    }


    @Override
    protected void hookOnCancel(Flow.Subscription subscription) {
        subscription.cancel();
    }

    @Override
    public String toString() {
        return "FlatMapProcessor{" +
                "mapper=" + mapper +
                ", subscriber=" + subscriber +
                '}';
    }
}
