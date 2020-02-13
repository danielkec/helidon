/*
 * Copyright (c) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
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

import java.util.Objects;

/**
 * Processor of {@link Multi} to {@link Single} that collects items from the {@link Multi} and publishes a single collector object
 * as a {@link Single}.
 *
 * @param <T> subscribed type (collected)
 * @param <U> published type (collector)
 */
final class MultiCollectingProcessor<T, U> extends BaseProcessor<T, U> implements Single<U> {

    private final Collector<T, U> collector;

    MultiCollectingProcessor(Collector<T, U> collector) {
        this.collector = Objects.requireNonNull(collector, "collector is null!");
    }

    @Override
    protected void next(T item) {
        try {
            collector.collect(item);
        } catch (Throwable t) {
            super.setError(t);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void submit(T item) {
        getSubscriber().onNext((U) item);
    }

    @Override
    public void onComplete() {
        U value;
        try {
            value = collector.value();
        } catch (Throwable t) {
            super.complete(t);
            return;
        }
        if (value == null) {
            super.onError(new IllegalStateException("Collector returned a null container"));
        } else if (getError() != null) {
            super.complete(getError());
        } else {
            getSubscriber().onNext(value);
            super.complete();
        }
    }
}
