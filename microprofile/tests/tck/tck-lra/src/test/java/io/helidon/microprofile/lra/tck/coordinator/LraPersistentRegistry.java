package io.helidon.microprofile.lra.tck.coordinator;

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

    Stream<LRA> stream() {
        return new HashSet<>(lraMap.values()).stream();
    }

    synchronized void load() {
        try {
            if (Files.exists(registry)) {
                JAXBContext context = JAXBContext.newInstance(LraPersistentRegistry.class, LRA.class, Participant.class);
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
            throw new RuntimeException(e);
        }
    }

    synchronized void save() {
        try {
            JAXBContext context = JAXBContext.newInstance(LraPersistentRegistry.class, LRA.class, Participant.class);
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
