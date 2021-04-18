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

public class LRAThreadContext {
    static ThreadLocal<LRAThreadContext> threadLocal = new ThreadLocal<>();

    private URI lra;
    private boolean ending = true;

    static synchronized LRAThreadContext get() {
        LRAThreadContext instance = threadLocal.get();
        if (instance == null) {
            instance = new LRAThreadContext();
            threadLocal.set(instance);
        }
        return instance;
    }

    public void lra(URI lra) {
        this.lra = lra;
    }

    public URI lra() {
        return lra;
    }

    public boolean ending() {
        return ending;
    }

    public void ending(boolean ending) {
        this.ending = ending;
    }
}
