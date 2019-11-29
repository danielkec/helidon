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

import java.util.function.Predicate;

public class DropWhileProcessor<T> extends BaseProcessor<T, T> implements Multi<T> {
    private Predicate<T> predicate;

    private boolean foundNotMatching = false;

    /**
     * Drop the longest prefix of elements from this stream that satisfy the given predicate.
     *
     * @param predicate provided predicate to filter stream with
     */
    public DropWhileProcessor(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void hookOnCancel(Flow.Subscription subscription) {
        subscription.cancel();
    }

    @Override
    protected void hookOnNext(T item) {
        try {
            if (foundNotMatching || !predicate.test(item)) {
                foundNotMatching = true;
                submit(item);
            } else {
                tryRequest(getSubscription());
            }
        } catch (Throwable t) {
            getSubscription().cancel();
            onError(t);
        }
    }
}
