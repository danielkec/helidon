/*
 * Copyright (c)  2020 Oracle and/or its affiliates.
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

package io.helidon.messaging.connectors.jms;

import java.io.Serializable;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;

class MessageMappers {

    private MessageMappers() {
    }

    private static final Map<Class<?>, MessageMapper> JMS_MESSAGE_MAPPERS = Map.of(
            String.class, (s, m) -> s.createTextMessage((String) m.getPayload()),
            Map.class, (s, m) -> {
                MapMessage mapm = s.createMapMessage();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) m.getPayload()).entrySet()) {
                    mapm.setObject(String.valueOf(e.getKey()), e.getValue());
                }
                return mapm;
            },
            byte[].class, (s, m) -> {
                BytesMessage bm = s.createBytesMessage();
                bm.writeBytes((byte[]) m.getPayload());
                return bm;
            }
    );

    static MessageMapper getJmsMessageMapper(org.eclipse.microprofile.reactive.messaging.Message<?> message) {
        Class<?> clazz = message.getPayload().getClass();
        return JMS_MESSAGE_MAPPERS
                .getOrDefault(clazz,
                        (s, o) -> {
                            if (o instanceof Serializable) {
                                return s.createObjectMessage((Serializable) o);
                            }
                            throw new JMSException("Unsupported payload type " + clazz);
                        });
    }

    @FunctionalInterface
    interface MessageMapper {
        Message apply(Session s, org.eclipse.microprofile.reactive.messaging.Message<?> m) throws JMSException;
    }
}
