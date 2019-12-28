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

package io.helidon.common.reactive;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Publisher from iterable, implemented as trampoline stack-less recursion.
 *
 * @param <T> item type
 */
public class OfPublisher<T> implements Flow.Publisher<T> {
    private Iterable<T> iterable;
    private AtomicBoolean cancelled = new AtomicBoolean(false);
    private AtomicBoolean completed = new AtomicBoolean(false);
    private AtomicBoolean trampolineLock = new AtomicBoolean(false);
    private final RequestedCounter requestCounter = new RequestedCounter();
    private final ReentrantLock iterateConcurrentLock = new ReentrantLock();

    /**
     * Create new {@link OfPublisher}.
     *
     * @param iterable to create publisher from
     */
    public OfPublisher(Iterable<T> iterable) {
        this.iterable = iterable;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        final Iterator<T> iterator;
        try {
            iterator = iterable.iterator();
        } catch (Throwable t) {
            subscriber.onError(t);
            return;
        }
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                requestCounter.increment(n, subscriber::onError);
                trySubmit();
            }

            private void trySubmit() {
                if (!trampolineLock.getAndSet(true)) {
                    try {
                        while (requestCounter.tryDecrement()) {
                            iterateConcurrentLock.lock();
                            try {
                                if (iterator.hasNext() && !cancelled.get()) {
                                    T next = iterator.next();
                                    Objects.requireNonNull(next);
                                    subscriber.onNext(next);
                                } else {
                                    if (!completed.getAndSet(true)) {
                                        subscriber.onComplete();
                                    }
                                    break;
                                }
                            } finally {
                                iterateConcurrentLock.unlock();
                            }
                        }
                    } finally {
                        trampolineLock.set(false);
                    }
                }
            }

            @Override
            public void cancel() {
                cancelled.set(true);
            }
        });
    }

}
