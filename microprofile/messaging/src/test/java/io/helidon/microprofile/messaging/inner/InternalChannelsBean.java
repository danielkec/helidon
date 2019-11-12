/*
 * Copyright (c) 2019 Oracle and/or its affiliates. All rights reserved.
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

package io.helidon.microprofile.messaging.inner;

import io.helidon.microprofile.messaging.CountableTestBean;
import io.helidon.microprofile.reactive.MultiRS;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@ApplicationScoped
public class InternalChannelsBean implements CountableTestBean {

    private static Set<String> TEST_DATA = new HashSet<>(Arrays.asList("test1", "test2"));
    public static CountDownLatch testLatch = new CountDownLatch(TEST_DATA.size());

    @Outgoing("intenal-publisher-string")
    public Publisher<String> produceMessage() {
        return MultiRS.just(TEST_DATA.stream());
    }

    @Incoming("intenal-publisher-string")
    public void receiveMethod(String msg) {
        if (TEST_DATA.contains(msg)) {
            testLatch.countDown();
        }
    }

    @Override
    public CountDownLatch getTestLatch() {
        return testLatch;
    }
}
