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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.Path;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MediaType;
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
    private final Map<Class<? extends Annotation>, Set<Method>> methodMap;

    private Participant(URI baseUri, Class<?> resourceClazz) {
        methodMap = scanForLRAMethods(resourceClazz);
        methodMap.entrySet().stream()
                .filter(e -> e.getKey() != LRA.class)
                .forEach(e -> {
                    Method method = e.getValue().stream().findFirst().get();
                    UriBuilder builder = UriBuilder.fromUri(baseUri)
                            .path(resourceClazz);

                    if (method.getAnnotation(Path.class) != null) {
                        builder.path(resourceClazz, method.getName());
                    }

                    URI uri = builder.build();
                    compensatorLinks.put(e.getKey(), uri);
                });
    }

    public boolean isLraMethod(Method m) {
        return methodMap.values().stream().flatMap(s -> s.stream()).anyMatch(m::equals);
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

    public static Optional<Annotation> getLRAAnnotation(Method m) {
        List<Annotation> found = Arrays.stream(m.getDeclaredAnnotations())
                .filter(a -> LRA_ANNOTATIONS.contains(a.annotationType()))
                .collect(Collectors.toList());

        if (found.size() > 1) {
            // TODO: LRA + Leave is OK
            //throw new IllegalStateException("Only one LRA annotation on the method is allowed " + m.getDeclaringClass() + "#" + m.getName());
        }

        if (found.size() == 0) {
            // LRA can be inherited from class or its predecesors
            var clazz = m.getDeclaringClass();
            do {
                LRA clazzLraAnnotation = clazz.getAnnotation(LRA.class);
                if (clazzLraAnnotation != null) {
                    return Optional.of(clazzLraAnnotation);
                }
                clazz = clazz.getSuperclass();
            } while (clazz != null);
        }

        return found.stream().findFirst();
    }

    public String compenstorLinks() {
        return Map.of(
                "compensate", compensate(),
                "complete", complete(),
                "forget", forget(),
                "leave", leave(),
                "after", after(),
                "status", status()
        )
                .entrySet()
                .stream()
                .filter(e -> e.getValue().isPresent())
                .map(e -> Link.fromUri(e.getValue().get())
                        .title(e.getKey() + " URI")
                        .rel(e.getKey())
                        .type(MediaType.TEXT_PLAIN)
                        .build())
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }

    public static Map<Class<? extends Annotation>, Set<Method>> scanForLRAMethods(Class<?> clazz) {
        Map<Class<? extends Annotation>, Set<Method>> methods = new HashMap<>();
        do {
            for (Method m : clazz.getDeclaredMethods()) {
                Optional<Annotation> annotation = getLRAAnnotation(m);
                if (annotation.isPresent()) {
                    methods.putIfAbsent(annotation.get().annotationType(), new HashSet<>());
                    methods.get(annotation.get().annotationType()).add(m);
                }
            }
            clazz = clazz.getSuperclass();
        } while (clazz != null);
        return methods;
    }

}
