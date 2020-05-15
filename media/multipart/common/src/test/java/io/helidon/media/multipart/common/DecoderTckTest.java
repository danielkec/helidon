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

import java.util.concurrent.Flow;
import java.util.stream.LongStream;

import io.helidon.common.http.DataChunk;
import io.helidon.common.reactive.Multi;

import static io.helidon.media.multipart.common.BodyPartTest.MEDIA_CONTEXT;

import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;

public class DecoderTckTest extends FlowPublisherVerification<DataChunk> {

    public DecoderTckTest() {
        super(new TestEnvironment(200));
    }

    @Override
    public Flow.Publisher<DataChunk> createFlowPublisher(final long l) {
//        MultiPartDecoder decoder = MultiPartDecoder.create("boundary", MEDIA_CONTEXT.readerContext());
//        Multi.from(LongStream.range(1, l)
//                .mapToObj(i ->
//                        WriteableBodyPart.builder()
//                                .entity("part" + i)
//                                .build()
//                )).subscribe(enc);
//        return enc;
        return null;
    }

    @Override
    public Flow.Publisher<DataChunk> createFailedFlowPublisher() {
        return null;
    }

    static Multi<DataChunk> chunksPublisher(byte[]... bytes) {
        DataChunk[] chunks = new DataChunk[bytes.length];
        for (int i=0 ; i < bytes.length ; i++) {
            chunks[i] = DataChunk.create(bytes[i]);
        }
        return Multi.just(chunks);
    }
}
