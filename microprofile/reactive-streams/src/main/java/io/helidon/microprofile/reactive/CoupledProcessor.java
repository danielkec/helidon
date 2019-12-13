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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;

public class CoupledProcessor<T, R> implements Processor<T, R> {

    private Subscriber<T> subscriber;
    private Publisher<T> publisher;
    private Subscriber<? super R> downStreamSubscriber;
    private Subscription upStreamSubscription;
    private Subscription downStreamsSubscription;


    public CoupledProcessor(Subscriber<T> subscriber, Publisher<T> publisher) {
        this.subscriber = subscriber;
        this.publisher = publisher;
    }

    @Override
    public void subscribe(Subscriber<? super R> downStreamSubscriber) {

        this.downStreamSubscriber = downStreamSubscriber;
        publisher.subscribe(new Subscriber<T>() {

            @Override
            public void onSubscribe(Subscription downStreamsSubscription) {
                Objects.requireNonNull(downStreamsSubscription);
                CoupledProcessor.this.downStreamsSubscription = downStreamsSubscription;
            }

            @Override
            @SuppressWarnings("unchecked")
            public void onNext(T t) {
                Objects.requireNonNull(t);
                downStreamSubscriber.onNext((R) t);
            }

            @Override
            public void onError(Throwable t) {
                Objects.requireNonNull(t);
                upStreamSubscription.cancel();
                subscriber.onError(t);
                downStreamSubscriber.onError(t);
            }

            @Override
            public void onComplete() {
                downStreamSubscriber.onComplete();
            }
        });

        downStreamSubscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                downStreamsSubscription.request(n);
            }

            @Override
            public void cancel() {
                subscriber.onComplete();
                downStreamsSubscription.cancel();
            }
        });
    }

    @Override
    public void onSubscribe(Subscription upStreamSubscription) {
        Objects.requireNonNull(upStreamSubscription);
        // https://github.com/reactive-streams/reactive-streams-jvm#2.5
        if (Objects.nonNull(this.upStreamSubscription)) {
            upStreamSubscription.cancel();
        }
        this.upStreamSubscription = upStreamSubscription;
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                upStreamSubscription.request(n);
            }

            @Override
            public void cancel() {
                upStreamSubscription.cancel();
                //downStreamsSubscription.cancel();//LIVELOCK!!!
                downStreamSubscriber.onComplete();
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onNext(T t) {
        subscriber.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        subscriber.onError(t);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
        downStreamSubscriber.onComplete();
        downStreamsSubscription.cancel();
    }
}