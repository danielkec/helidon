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
 */
package io.helidon.microprofile.lra;

import org.eclipse.microprofile.lra.LRAResponse;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

@ApplicationScoped
public class ParticipantService {

    @Inject
    private LRACdiExtension lraCdiExtension;

    @Inject
    BeanManager beanManager;
    
    final Map<Class<?>, Participant> participants = new HashMap<>();

    Participant participant(URI baseUri, Class<?> clazz) {
        return participants.computeIfAbsent(clazz, c -> new Participant(baseUri, c));
    }

    /**
     * Participant ID is expected to be classFqdn#methodName
     */
    Response invoke(String classFqdn, String methodName, URI lraId, URI parentId) throws InvocationTargetException {
        Class<?> clazz;
        try {
            clazz = Class.forName(classFqdn);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cant locate participant method: " + classFqdn + "#" + methodName, e);
        }

        try {
            Bean<?> bean = lraCdiExtension.lraCdiBeanReferences().get(clazz);
            Objects.requireNonNull(bean, () -> "Missing bean reference for participant method: " + classFqdn + "#" + methodName);
            Method method = Arrays.stream(clazz.getDeclaredMethods())
                    .filter(m -> m.getName().equals(methodName)) //TODO: filter those with right annotation
                    .findFirst().orElseThrow(() -> new RuntimeException("Cant find participant method " + methodName
                            + " with participant method: " + classFqdn + "#" + methodName));
            int paramCount = method.getParameters().length;
            Object result = method.invoke(LRACdiExtension.lookup(bean, beanManager), 
                    Stream.of(lraId, parentId).limit(paramCount).toArray());
            return (Response) result;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cant invoke participant method " + methodName
                    + " with participant method: " + classFqdn + "#" + methodName, e);
        }
    }
}
