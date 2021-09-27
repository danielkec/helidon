package io.helidon.lra.coordinator.client.narayana;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.helidon.lra.coordinator.client.Participant;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_ENDED_CONTEXT_HEADER;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TMMTest {

    static final Duration TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    static final String COORDINATOR_URL = "http://localhost:8070/lra-coordinator";

    static ConcurrentHashMap<String, AtomicBoolean> completed = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, AtomicBoolean> aftered = new ConcurrentHashMap<>();
    private static NarayanaClient narayanaClient;
    private static int port;
    private static WebServer server;

    @BeforeAll
    static void setUp() {
        server = WebServer.builder()
                .port(0)//random port
                .routing(Routing.builder()
                        .any((req, res) -> {
                            System.out.println("Coordinator ====> " + req.path().toRawString() + " \n" + req.headers()
                                    .toMap()
                                    .entrySet()
                                    .stream()
                                    .filter(e -> List.of(LRA_HTTP_CONTEXT_HEADER, LRA_HTTP_ENDED_CONTEXT_HEADER).contains(e.getKey()))
                                    .map(e -> e.getKey() + ": " + e.getValue())
                                    .collect(Collectors.joining("\n"))
                            );
                            switch (req.path().segments().get(1)) {
                                case "complete":
                                    req.headers()
                                            .first(LRA_HTTP_CONTEXT_HEADER)
                                            .ifPresent(id -> completed.getOrDefault(id, new AtomicBoolean()).set(true));
                                    break;
                                case "after":
                                    req.headers()
                                            .first(LRA_HTTP_ENDED_CONTEXT_HEADER)
                                            .ifPresent(id -> aftered.getOrDefault(id, new AtomicBoolean()).set(true));
                                    break;
                            }
                            res.status(200).send();
                        })
                        .build())
                .build()
                .start()
                .await(TIMEOUT);

        port = server.port();

        narayanaClient = new NarayanaClient();
        narayanaClient.init(() -> URI.create(COORDINATOR_URL), 3, TimeUnit.SECONDS);
    }

    @AfterAll
    static void afterAll() {
        server.shutdown().await(TIMEOUT);
    }

    @RepeatedTest(10)
    void nestedAfterLra(RepetitionInfo repetitionInfo) throws InterruptedException {
        completed.clear();
        aftered.clear();

        var parentLra = startLra(null, "parent");
        completed.put(parentLra.toASCIIString(), new AtomicBoolean(false));
        aftered.put(parentLra.toASCIIString(), new AtomicBoolean(false));
        System.out.println("Started Parent " + parentLra.toASCIIString());


        var nestedLra = startLra(parentLra, "nested");
        completed.put(nestedLra.toASCIIString(), new AtomicBoolean(false));
        aftered.put(nestedLra.toASCIIString(), new AtomicBoolean(false));
        System.out.println("Started Nested " + nestedLra.toASCIIString());
        System.out.println("Closing Nested " + nestedLra.toASCIIString());
        closeLra(nestedLra, "nested");

        int sleepSec = repetitionInfo.getCurrentRepetition();
        System.out.println("Sleeping for: " + sleepSec);
        TimeUnit.SECONDS.sleep(sleepSec);

        System.out.println("Closing Parent " + parentLra.toASCIIString());
        closeLra(parentLra, "parent");
        completed.forEach((key, value) -> assertTrue(value.get(), "Should have completed already! " + key));
        aftered.forEach((key, value) -> assertTrue(value.get(), "Should have aftered already! " + key));
    }

    void closeLra(URI lra, String participant) {
        narayanaClient.join(lra, 0, testParticipant(participant)).await(TIMEOUT);
        narayanaClient.close(lra).await(TIMEOUT);
    }

    URI startLra(URI parent, String participant) {
        URI lraId;
        if (parent != null) {
            lraId = narayanaClient.start(parent, participant, 0).await(TIMEOUT);
        } else {
            lraId = narayanaClient.start(participant, 0).await(TIMEOUT);
        }

        narayanaClient.join(lraId, 0, testParticipant(participant)).await(TIMEOUT);
        return lraId;
    }

    static Participant testParticipant(String name) {
        String target = "http://localhost:" + port + "/" + name;
        return new Participant() {
            @Override
            public Optional<URI> compensate() {
                return Optional.of(URI.create(target + "/compensate"));
            }

            @Override
            public Optional<URI> complete() {
                return Optional.of(URI.create(target + "/complete"));
            }

            @Override
            public Optional<URI> forget() {
                return Optional.of(URI.create(target + "/forget"));
            }

            @Override
            public Optional<URI> leave() {
                return Optional.of(URI.create(target + "/leave"));
            }

            @Override
            public Optional<URI> after() {
                return Optional.of(URI.create(target + "/after"));
            }

            @Override
            public Optional<URI> status() {
                return Optional.of(URI.create(target + "/status"));
            }
        };
    }
}
