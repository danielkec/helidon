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

package io.helidon.microprofile.lra.coordinator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import javax.ws.rs.core.Link;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class LraPersistentRegistry {

    static java.nio.file.Path registry = Paths.get("target/mock-coordinator/lra-registry");

    private final Map<String, LRA> lraMap = new HashMap<>();

    synchronized LRA get(String lraId) {
        return lraMap.get(lraId);
    }

    synchronized void put(String key, LRA lra) {
        lraMap.put(key, lra);
    }

    int size() {
        return lraMap.size();
    }

    synchronized void remove(String key) {
        lraMap.remove(key);
    }

    synchronized Stream<LRA> stream() {
        return new HashSet<>(lraMap.values()).stream();
    }

    synchronized void load() {
        try {
            if (Files.exists(registry)) {
                JAXBContext context = JAXBContext.newInstance(
                        LraPersistentRegistry.class, 
                        LRA.class, 
                        Participant.class,
                        LRAStatus.class,
                        Participant.AfterLraStatus.class,
                        ParticipantStatus.class);
                Unmarshaller unmarshaller = context.createUnmarshaller();
                unmarshaller.setAdapter(new Link.JaxbAdapter());
                LraPersistentRegistry lraPersistentRegistry = (LraPersistentRegistry) unmarshaller.unmarshal(registry.toFile());
                lraMap.putAll(lraPersistentRegistry.lraMap);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println("+++++++++++++++++++");
            try {
                System.out.println(Files.readString(registry));
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            System.out.println("+++++++++++++++++++");
        }
    }

    synchronized void save() {
        try {
            JAXBContext context = JAXBContext.newInstance(
                    LraPersistentRegistry.class,
                    LRA.class, 
                    Participant.class, 
                    LRAStatus.class,
                    Participant.AfterLraStatus.class,
                    ParticipantStatus.class);
            Marshaller mar = context.createMarshaller();
            mar.setAdapter(new Link.JaxbAdapter());
            mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            if (!Files.exists(registry.getParent())) Files.createDirectories(registry.getParent());
            Files.deleteIfExists(registry);
            Files.createFile(registry);
            LraPersistentRegistry lraPersistentRegistry = new LraPersistentRegistry();
            lraPersistentRegistry.lraMap.putAll(lraMap);
            mar.marshal(lraPersistentRegistry, registry.toFile());
        } catch (JAXBException | IOException e) {
            e.printStackTrace();
        }
    }
}
