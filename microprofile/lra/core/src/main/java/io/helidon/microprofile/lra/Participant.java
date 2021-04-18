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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.ws.rs.core.UriBuilder;

import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

public class Participant {

    static final Set<Class<? extends Annotation>> LRA_ANNOTATIONS =
            Set.of(LRA.class, Compensate.class, Complete.class, Forget.class, Status.class, AfterLRA.class, Leave.class);

    static final Map<Class<?>, Participant> participants = new HashMap<>();

    private final Map<Class<? extends Annotation>, URI> compensatorLinks = new HashMap<>();

    private Participant(URI baseUri, Class<?> resourceClazz) {
        Map<Class<? extends Annotation>, Method> methodMap = scanForLRAMethods(resourceClazz);
        methodMap.forEach((annotation, method) -> compensatorLinks.put(annotation,
                UriBuilder.fromUri(baseUri)
                        .path(resourceClazz)
                        .path(resourceClazz, method.getName())
                        .build()
        ));
    }

    public Optional<URI> compensate() {
        return Optional.ofNullable(compensatorLinks.get(Compensate.class));
    }

    public Optional<URI> complete() {
        return Optional.ofNullable(compensatorLinks.get(Complete.class));
    }

    public Optional<URI> forget() {
        return Optional.ofNullable(compensatorLinks.get(Forget.class));
    }

    public Optional<URI> leave() {
        return Optional.ofNullable(compensatorLinks.get(Leave.class));
    }

    public Optional<URI> after() {
        return Optional.ofNullable(compensatorLinks.get(AfterLRA.class));
    }

    public Optional<URI> status() {
        return Optional.ofNullable(compensatorLinks.get(Status.class));
    }

    public static Participant get(URI baseUri, Class<?> clazz) {
        return participants.computeIfAbsent(clazz, c -> new Participant(baseUri, c));
    }

    private static Map<Class<? extends Annotation>, Method> scanForLRAMethods(Class<?> clazz) {
        Map<Class<? extends Annotation>, Method> methods = new HashMap<>();
        do {
            for (Method m : clazz.getDeclaredMethods()) {
                Optional<? extends Class<? extends Annotation>> annotation = getLRAAnnotation(m);
                annotation.ifPresent(aClass -> methods.putIfAbsent(aClass, m));
            }
            clazz = clazz.getSuperclass();
        } while (clazz != null);
        return methods;
    }

    private static Optional<? extends Class<? extends Annotation>> getLRAAnnotation(Method m) {
        return Stream.of(m.getDeclaredAnnotations())
                .map(Annotation::annotationType)
                .filter(LRA_ANNOTATIONS::contains)
                .findFirst();
    }
}
