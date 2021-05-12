/*
 * Copyright (c) 2021 Oracle and/or its affiliates.
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

package io.helidon.microprofile.lra;

import java.net.URI;
import java.util.Optional;

class LRAThreadContext {

    private static final ThreadLocal<LRAThreadContext> LRA_THREAD_CONTEXT = new ThreadLocal<>();

    private URI lra;

    static synchronized LRAThreadContext get() {
        LRAThreadContext instance = LRA_THREAD_CONTEXT.get();
        if (instance == null) {
            instance = new LRAThreadContext();
            LRA_THREAD_CONTEXT.set(instance);
        }
        return instance;
    }

    static void clear() {
        LRA_THREAD_CONTEXT.set(null);
    }

    void lra(URI lra) {
        this.lra = lra;
    }

    Optional<URI> lra() {
        return Optional.ofNullable(lra);
    }
}
