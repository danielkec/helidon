/*
 * Copyright (c) 2020 Oracle and/or its affiliates. All rights reserved.
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
package io.helidon.common.reactive;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.testng.Assert;

public class MultiConcatArrayTest {

    private static ExecutorService exec;

    @BeforeAll
    static void beforeAll() {
        exec = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void afterAll() {
        exec.shutdown();
    }

    @Test
    public void errors() {
        TestSubscriber<Object> ts = new TestSubscriber<>(Long.MAX_VALUE);

        Multi.concatArray(Multi.singleton(1), Multi.error(new IOException()), Multi.singleton(2))
                .subscribe(ts);

        ts.assertFailure(IOException.class, 1);
    }

    @Test
    public void millionSources() {
        @SuppressWarnings("unchecked")
        Multi<Integer>[] sources = new Multi[1_000_000];
        Arrays.fill(sources, Multi.singleton(1));

        TestSubscriber<Object> ts = new TestSubscriber<>(Long.MAX_VALUE);

        Multi.concatArray(sources)
                .subscribe(ts);

        ts.assertItemCount(1_000_000)
                .assertComplete();
    }

    @RepeatedTest(4000000)
    public void switchOverRequestRace() {
        TestSubscriber<Object> ts = new TestSubscriber<>(0L);
        AtomicReference<Flow.Subscriber<? super Integer>> sref = new AtomicReference<>();

        Multi.concatArray(subscriber -> {
            subscriber.onSubscribe(EmptySubscription.INSTANCE);
            sref.set(subscriber);
        }, Multi.range(1, 100)).subscribe(ts);

        race(
                () -> sref.get().onComplete(),
                () -> {
                    var s = ts.getSubcription();
                    for (int j = 0; j < 100; j++) {
                        s.request(1);
                    }
                }, exec);

        ts.awaitDone(3, TimeUnit.SECONDS);
        ts.assertItemCount(100);
    }

    private static void race(Runnable r1, Runnable r2, ExecutorService exec) {

        AtomicInteger sync = new AtomicInteger(2);
        CountDownLatch cdl = new CountDownLatch(1);

        exec.submit(() -> {
            if (sync.decrementAndGet() != 0) {
                while (sync.get() != 0) ;
            }

            try {
                r1.run();
            } finally {
                cdl.countDown();
            }
        });

        if (sync.decrementAndGet() != 0) {
            while (sync.get() != 0) ;
        }
        r2.run();

        try {
            Assert.assertTrue(cdl.await(50, TimeUnit.SECONDS));
        } catch (InterruptedException ex) {
            Assert.fail("Race test got interrupted", ex);
        }
    }
}
