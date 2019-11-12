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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletionStage;

/**
 * Replacement for buggy DefaultCompletionSubscriber
 * <p>
 * https://github.com/eclipse/microprofile-reactive-streams-operators/issues/129#issue-521492223
 *
 * @param <T>
 * @param <R>
 */
public class CompletionSubscriber<T, R> implements org.eclipse.microprofile.reactive.streams.operators.CompletionSubscriber<T, R> {

    private final Subscriber<T> subscriber;
    private final CompletionStage<R> completion;

    public static <T, R> CompletionSubscriber<T, R> of(Subscriber<T> subscriber, CompletionStage<R> completion) {
        return new CompletionSubscriber<>(subscriber, completion);
    }

    private CompletionSubscriber(Subscriber<T> subscriber, CompletionStage<R> completion) {
        this.subscriber = subscriber;
        this.completion = completion;
    }

    @Override
    public CompletionStage<R> getCompletion() {
        return completion;
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscriber.onSubscribe(s);
    }

    @Override
    public void onNext(T t) {
        subscriber.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        subscriber.onError(t);
        completion.toCompletableFuture().completeExceptionally(t);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
        completion.toCompletableFuture().complete(null);
    }
}
