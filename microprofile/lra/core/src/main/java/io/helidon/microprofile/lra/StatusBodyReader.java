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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;

import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

abstract class StatusBodyReader<T extends Enum<T>> implements MessageBodyReader<T> {

    abstract Class<T> enumType();

    @Override
    public boolean isReadable(Class<?> type,
                              Type genericType,
                              Annotation[] annotations,
                              MediaType mediaType) {
        return enumType().isAssignableFrom(type);
    }

    @Override
    public T readFrom(Class<T> type,
                      Type genericType,
                      Annotation[] annotations,
                      MediaType mediaType,
                      MultivaluedMap<String, String> httpHeaders,
                      InputStream entityStream) throws WebApplicationException {

        String textBody = new BufferedReader(new InputStreamReader(entityStream))
                .lines()
                .collect(Collectors.joining("\n"));

        return Enum.valueOf(enumType(), textBody);
    }

    static class LRAStatusBodyReader extends StatusBodyReader<LRAStatus> {
        @Override
        Class<LRAStatus> enumType() {
            return LRAStatus.class;
        }
    }

    static class ParticipantStatusBodyReader extends StatusBodyReader<ParticipantStatus> {
        @Override
        Class<ParticipantStatus> enumType() {
            return ParticipantStatus.class;
        }
    }
}
