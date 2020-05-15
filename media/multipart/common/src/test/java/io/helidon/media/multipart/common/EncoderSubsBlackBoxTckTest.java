/*
 * Copyright (c)  2020 Oracle and/or its affiliates. All rights reserved.
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

package io.helidon.media.multipart.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import static io.helidon.media.multipart.common.BodyPartTest.MEDIA_CONTEXT;

import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowSubscriberBlackboxVerification;

import io.helidon.common.http.DataChunk;

public class EncoderSubsBlackBoxTckTest extends FlowSubscriberBlackboxVerification<WriteableBodyPart> {

    protected EncoderSubsBlackBoxTckTest() {
        super(new TestEnvironment(200));
    }

    @Override
    public Flow.Subscriber<WriteableBodyPart> createFlowSubscriber() {
        Consumer<Flow.Subscription> run = s -> s.request(Long.MAX_VALUE);
        CompletableFuture<Void> future = new CompletableFuture<>();
        var enc = new MultiPartEncoder("boundary", MEDIA_CONTEXT.writerContext()){
            @Override
            public void onSubscribe(final Flow.Subscription subscription) {
                super.onSubscribe(subscription);
                future.complete(null);
            }
        };
        enc.subscribe(new Flow.Subscriber<DataChunk>() {
            @Override
            public void onSubscribe(final Flow.Subscription subscription) {
                future.whenComplete((aVoid, throwable) -> run.accept(subscription));
            }

            @Override
            public void onNext(final DataChunk item) {

            }

            @Override
            public void onError(final Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });
        return enc;
    }

    @Override
    public WriteableBodyPart createElement(final int element) {
        return WriteableBodyPart.builder()
                .entity("part" + element)
                .build();
    }
}
