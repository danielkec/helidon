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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

import io.helidon.common.reactive.DistinctProcessor;
import io.helidon.common.reactive.DropWhileProcessor;
import io.helidon.common.reactive.FilterProcessor;
import io.helidon.common.reactive.Flow;
import io.helidon.common.reactive.LimitProcessor;
import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.PeekProcessor;
import io.helidon.common.reactive.SkipProcessor;
import io.helidon.common.reactive.TakeWhileProcessor;
import io.helidon.microprofile.reactive.hybrid.HybridProcessor;
import io.helidon.microprofile.reactive.hybrid.HybridSubscriber;

import org.eclipse.microprofile.reactive.streams.operators.CompletionSubscriber;
import org.eclipse.microprofile.reactive.streams.operators.spi.Graph;
import org.eclipse.microprofile.reactive.streams.operators.spi.Stage;
import org.eclipse.microprofile.reactive.streams.operators.spi.SubscriberWithCompletionStage;
import org.eclipse.microprofile.reactive.streams.operators.spi.UnsupportedStageException;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public final class GraphBuilder extends HashMap<Class<? extends Stage>, Consumer<Stage>> {

    private Multi<Object> multi = null;
    private List<Flow.Processor<Object, Object>> processorList = new ArrayList<>();
    private CompletionStage<Object> completionStage = null;
    private SubscriberWithCompletionStage<Object, Object> subscriberWithCompletionStage = null;

    @SuppressWarnings("unchecked")
    private GraphBuilder() {
        registerStage(Stage.PublisherStage.class, stage -> {
            multi = MultiRS.toMulti((Publisher<Object>) stage.getRsPublisher());
        });
        registerStage(Stage.Concat.class, stage -> {
            HelidonReactiveStreamEngine streamEngine = new HelidonReactiveStreamEngine();
            Publisher<Object> firstPublisher = streamEngine.buildPublisher(stage.getFirst());
            Publisher<Object> secondPublisher = streamEngine.buildPublisher(stage.getSecond());
            multi = MultiRS.toMulti(new ConcatPublisher<>(firstPublisher, secondPublisher));
        });
        registerStage(Stage.Of.class, stage -> {
            multi = Multi.from(new OfPublisher(stage.getElements()));
        });
        registerStage(Stage.Failed.class, stage -> {
            multi = Multi.from(new FailedPublisher(stage.getError()));
        });
        registerStage(Stage.FromCompletionStage.class, stage -> {
            multi = MultiRS.toMulti(new FromCompletionStagePublisher(stage.getCompletionStage(), false));
        });
        registerStage(Stage.FromCompletionStageNullable.class, stage -> {
            multi = MultiRS.toMulti(new FromCompletionStagePublisher(stage.getCompletionStage(), true));
        });
        registerStage(Stage.Map.class, stage -> {
            Function<Object, Object> mapper = (Function<Object, Object>) stage.getMapper();
            processorList.add(new MapProcessor<>(mapper::apply));
        });
        registerStage(Stage.Filter.class, stage -> {
            processorList.add(new FilterProcessor(stage.getPredicate()));
        });
        registerStage(Stage.TakeWhile.class, stage -> {
            processorList.add(new TakeWhileProcessor(stage.getPredicate()));
        });
        registerStage(Stage.DropWhile.class, stage -> {
            processorList.add(new DropWhileProcessor(stage.getPredicate()));
        });
        registerStage(Stage.Peek.class, stage -> {
            Consumer<Object> peekConsumer = (Consumer<Object>) stage.getConsumer();
            processorList.add(new PeekProcessor<>(peekConsumer));
        });
        registerStage(Stage.Limit.class, stage -> {
            processorList.add(new LimitProcessor(stage.getLimit()));
        });
        registerStage(Stage.Skip.class, stage -> {
            processorList.add(new SkipProcessor(stage.getSkip()));
        });
        registerStage(Stage.Distinct.class, stage -> {
            processorList.add(new DistinctProcessor<>());
        });
        registerStage(Stage.FlatMap.class, stage -> {
            processorList.add(new FlatMapProcessor(stage.getMapper()));
        });
        registerStage(Stage.OnTerminate.class, stage -> {
            processorList.add(TappedProcessor.create()
                    .onComplete(stage.getAction())
                    .onCancel((s) -> stage.getAction().run())
                    .onError((t) -> stage.getAction().run()));
        });
        registerStage(Stage.Cancel.class, stage -> {
            CancelSubscriber cancelSubscriber = new CancelSubscriber();
            subscribe(cancelSubscriber);
            this.subscriberWithCompletionStage =
                    RedeemingCompletionSubscriber.of(HybridSubscriber.from(cancelSubscriber),
                            CompletableFuture.completedFuture(null));
        });
        registerStage(Stage.FindFirst.class, stage -> {
            FindFirstSubscriber<Object> firstSubscriber = new FindFirstSubscriber<>();
            subscribe(firstSubscriber);
            this.subscriberWithCompletionStage = firstSubscriber;
        });
        registerStage(Stage.SubscriberStage.class, stage -> {
            Subscriber<Object> subscriber = (Subscriber<Object>) stage.getRsSubscriber();
            RedeemingCompletionSubscriber<Object, Object> completionSubscriber;
            if (subscriber instanceof CompletionSubscriber) {
                completionSubscriber = RedeemingCompletionSubscriber.of(subscriber, ((CompletionSubscriber) subscriber).getCompletion());
            } else {
                completionSubscriber = RedeemingCompletionSubscriber.of(subscriber, new CompletableFuture<>());
            }
            subscribe(completionSubscriber);
            this.subscriberWithCompletionStage = completionSubscriber;
        });
        registerStage(Stage.Collect.class, stage -> {
            // Foreach
            this.subscriberWithCompletionStage = new CollectSubscriber<Object>(stage, processorList);
            // If producer was supplied
            if (multi != null) {
                multi.subscribe(HybridSubscriber.from(subscriberWithCompletionStage.getSubscriber()));
            }
        });
    }

    public static GraphBuilder create() {
        return new GraphBuilder();
    }

    public GraphBuilder from(Graph graph) {
        graph.getStages().forEach(this::add);
        return this;
    }

    public GraphBuilder add(Stage stage) {
        Consumer<Stage> stageConsumer = this.get(stage.getClass());

        this.keySet()
                .stream()
                .filter(c -> c.isAssignableFrom(stage.getClass()))
                .map(this::get)
                .findFirst()
                .orElseThrow(() -> new UnsupportedStageException(stage))
                .accept(stage);

        return this;
    }

    /**
     * Return subscriber from even incomplete graph,
     * in case of incomplete graph does subscriptions downstream automatically in the
     * {@link CollectSubscriber}.
     *
     * @param <U> type of items subscriber consumes
     * @param <W> type of items subscriber emits
     * @return {@link org.eclipse.microprofile.reactive.streams.operators.spi.SubscriberWithCompletionStage}
     */
    @SuppressWarnings("unchecked")
    <U, W> SubscriberWithCompletionStage<U, W> getSubscriberWithCompletionStage() {
        return (SubscriberWithCompletionStage<U, W>) subscriberWithCompletionStage;
    }

    /**
     * Return {@link java.util.concurrent.CompletionStage}
     * either from supplied {@link org.reactivestreams.Subscriber}
     * for example by {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
     * or from completion stage for example
     * {@link org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder#forEach(java.util.function.Consumer)}.
     *
     * @param <U> type of items subscriber consumes
     * @return {@link CollectSubscriber}
     */
    @SuppressWarnings("unchecked")
    <U> CompletionStage<U> getCompletionStage() {
        return (CompletionStage<U>) (completionStage != null ? completionStage : subscriberWithCompletionStage.getCompletion());
    }

    /**
     * Return {@link org.reactivestreams.Processor} wrapping all processor stages from processor builder.
     * <p>See example:
     * <pre>{@code
     *   Processor<Integer, String> processor = ReactiveStreams.<Integer>builder()
     *       .map(i -> i + 1)
     *       .flatMap(i -> ReactiveStreams.of(i, i))
     *       .map(i -> Integer.toString(i))
     *       .buildRs();
     * }</pre>
     *
     * @param <T> type of items subscriber consumes
     * @param <R> type of items subscriber emits
     * @return {@link org.reactivestreams.Processor} wrapping all processor stages
     */
    @SuppressWarnings("unchecked")
    <T, R> Processor<T, R> getProcessor() {
        return (Processor<T, R>) new CumulativeProcessor(processorList);
    }

    /**
     * Returns {@link org.reactivestreams.Publisher} made from supplied stages.
     * <p>See example:
     * <pre>{@code
     *   ReactiveStreams
     *      .of("10", "20", "30")
     *      .map(a -> a.replaceAll("0", ""))
     *      .map(Integer::parseInt)
     *      .buildRs()
     * }</pre>
     *
     * @return {@link org.reactivestreams.Publisher}
     */
    @SuppressWarnings("unchecked")
    <T> Publisher<T> getPublisher() {
        subscribeUpStream();
        return (Publisher<T>) MultiRS.from(multi);
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    private void subscribeUpStream() {
        if (multi != null) {
            for (Flow.Processor p : processorList) {
                multi.subscribe(p);
                multi = (Multi<Object>) p;
            }
        } else {
            throw new RuntimeException("No producer was supplied");
        }
    }

    private void subscribe(Subscriber<Object> subscriber) {
        CumulativeProcessor cumulativeProcessor = new CumulativeProcessor(processorList);
        if (multi != null) {
            multi.subscribe(HybridProcessor.from(cumulativeProcessor));
        }
        cumulativeProcessor.subscribe(subscriber);
    }

    private void subscribe(Flow.Subscriber<Object> subscriber) {
        subscribe((Subscriber<Object>) HybridSubscriber.from(subscriber));
    }

    @SuppressWarnings("unchecked")
    private <S extends Stage> GraphBuilder registerStage(Class<S> stageType, Consumer<S> consumer) {
        this.put(stageType, (Consumer<Stage>) consumer);
        return this;
    }
}
