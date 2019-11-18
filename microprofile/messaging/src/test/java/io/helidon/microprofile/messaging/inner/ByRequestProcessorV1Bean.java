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

package io.helidon.microprofile.messaging.inner;

import io.helidon.microprofile.reactive.MultiRS;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

@ApplicationScoped
public class ByRequestProcessorV1Bean {

    public static CountDownLatch testLatch = new CountDownLatch(10);

    @Outgoing("inner-processor")
    public Publisher<Integer> produceMessage() {
        return MultiRS.just(IntStream.range(0, 10).boxed());
    }

    @Incoming("inner-processor")
    @Outgoing("inner-consumer")
    public int process(int i) {
        return i++;
    }

    @Incoming("inner-consumer")
    public void receiveMessage(int i) {
        testLatch.countDown();
    }

}