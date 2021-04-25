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

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import io.helidon.common.configurable.ScheduledThreadPoolSupplier;
import io.helidon.microprofile.config.ConfigCdiExtension;
import io.helidon.microprofile.lra.coordinator.Coordinator;
import io.helidon.microprofile.lra.coordinator.CoordinatorApplication;
import io.helidon.microprofile.scheduling.SchedulingCdiExtension;
import io.helidon.microprofile.server.JaxRsCdiExtension;
import io.helidon.microprofile.server.RoutingName;
import io.helidon.microprofile.server.ServerCdiExtension;
import io.helidon.microprofile.tests.junit5.AddBean;
import io.helidon.microprofile.tests.junit5.AddConfig;
import io.helidon.microprofile.tests.junit5.AddExtension;
import io.helidon.microprofile.tests.junit5.DisableDiscovery;
import io.helidon.microprofile.tests.junit5.HelidonTest;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.hamcrest.core.AnyOf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@HelidonTest
@DisableDiscovery
@AddExtension(ConfigCdiExtension.class)
@AddExtension(ServerCdiExtension.class)
@AddExtension(JaxRsCdiExtension.class)
@AddExtension(LRACdiExtension.class)
@AddExtension(CdiComponentProvider.class)
@AddBean(NarayanaClient.class)
@AddBean(NarayanaAdapter.class)
@AddBean(InspectionService.class)
@AddBean(TestApplication.class)
@AddBean(TestApplication.StartAndClose.class)
@AddBean(TestApplication.StartAndAfter.class)
@AddBean(TestApplication.DontEnd.class)
@AddBean(TestApplication.Timeout.class)
@AddBean(TestApplication.Recovery.class)

@AddBean(Coordinator.class)
@AddBean(CoordinatorApplication.class)
@AddExtension(SchedulingCdiExtension.class)
@AddConfig(key = "io.helidon.microprofile.lra.coordinator.CoordinatorApplication."
        + RoutingName.CONFIG_KEY_NAME, value = "coordinator")
@AddConfig(key = "server.sockets.0.name", value = "coordinator1")
@AddConfig(key = "server.sockets.0.port", value = "8070")
@AddConfig(key = "server.sockets.0.bind-address", value = "localhost")
public class BasicTest {

    private static ScheduledExecutorService executor;
    private final Map<String, CompletableFuture<URI>> completionMap = new HashMap<>();

    public synchronized CompletableFuture<URI> getCompletable(String key) {
        completionMap.putIfAbsent(key, new CompletableFuture<>());
        return completionMap.get(key);
    }

    @Inject
    CoordinatorClient coordinatorClient;

    @BeforeAll
    static void beforeAll() {
        executor = ScheduledThreadPoolSupplier.create().get();
    }

    @AfterAll
    static void afterAll() {
        executor.shutdownNow();
    }

    @Test
    void testInMethod(WebTarget target) throws Exception {
        Response response = target.path("start-and-close")
                .path("start")
                .request()
                .async()
                .put(Entity.text(""))
                .get(10, TimeUnit.SECONDS);
        assertThat(response.getStatus(), AnyOf.anyOf(is(200), is(204)));
        getCompletable("start-and-close").get(10, TimeUnit.SECONDS);
    }

    @Test
    void startAndAfter(WebTarget target) throws Exception {
        Response response = target.path("start-and-after")
                .path("start")
                .request()
                .async()
                .put(Entity.text(""))
                .get(2, TimeUnit.SECONDS);
        assertThat(response.getStatus(), AnyOf.anyOf(is(200), is(204)));
        getCompletable("start-and-after").get(2, TimeUnit.SECONDS);
    }

    @Test
    void firstNotEnding(WebTarget target) throws Exception {
        Response response = target.path("dont-end")
                .path("first-not-ending")
                .request()
                .async()
                .put(Entity.text(""))
                .get(10, TimeUnit.SECONDS);
        assertThat(response.getStatus(), AnyOf.anyOf(is(200), is(204)));
        URI lraId = getCompletable("first-not-ending").get(10, TimeUnit.SECONDS);
        assertThat(coordinatorClient.status(lraId), is(LRAStatus.Active));
        assertThat(target.path("dont-end")
                .path("second-ending")
                .request()
                .header(LRA_HTTP_CONTEXT_HEADER, lraId)
                .async()
                .put(Entity.text(""))
                .get(10, TimeUnit.SECONDS).getStatus(), AnyOf.anyOf(is(200), is(204)));
        getCompletable("second-ending").get(10, TimeUnit.SECONDS);
        assertClosedOrNotFound(lraId);
    }

    @Test
    void timeout(WebTarget target) throws Exception {
        Response response = target.path("timeout")
                .path("timeout")
                .request()
                .async()
                .put(Entity.text(""))
                .get(2, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(200));
        getCompletable("timeout").get(5, TimeUnit.SECONDS);
        getCompletable("timeout-compensated").get(5, TimeUnit.SECONDS);
    }

    @Test
    void compensateRecoveryTest(WebTarget target) throws ExecutionException, InterruptedException, TimeoutException {
        LocalDateTime start = LocalDateTime.now();
        Response response = target.path("recovery")
                .path("start-compensate")
                .request()
                .async()
                .put(Entity.text(""))
                .get(5, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(500));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(getCompletable("recovery-compensated-first").get(5, TimeUnit.SECONDS), is(lraId));
        LocalDateTime first = LocalDateTime.now();
        System.out.println("First compensate attempt after " + Duration.between(start, first));
        waitForRecovery(lraId);
        assertThat(getCompletable("recovery-compensated-second").get(8, TimeUnit.SECONDS), is(lraId));
        LocalDateTime second = LocalDateTime.now();
        System.out.println("Second compensate attempt after " + Duration.between(first, second));
    }

    @Test
    void completeRecoveryTest(WebTarget target) throws ExecutionException, InterruptedException, TimeoutException {
        LocalDateTime start = LocalDateTime.now();
        Response response = target.path("recovery")
                .path("start-complete")
                .request()
                .async()
                .put(Entity.text(""))
                .get(5, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(200));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(getCompletable("recovery-completed-first").get(2, TimeUnit.SECONDS), is(lraId));
        LocalDateTime first = LocalDateTime.now();
        System.out.println("First complete attempt after " + Duration.between(start, first));
        waitForRecovery(lraId);
        assertThat(getCompletable("recovery-completed-second").get(8, TimeUnit.SECONDS), is(lraId));
        LocalDateTime second = LocalDateTime.now();
        System.out.println("Second complete attempt after " + Duration.between(first, second));
    }

    private void assertClosedOrNotFound(URI lraId) {
        try {
            assertThat(coordinatorClient.status(lraId), is(LRAStatus.Closed));
        } catch (NotFoundException e) {
            // in case coordinator don't retain closed lra long enough
        }
    }
    
    private void waitForRecovery(URI lraId) {
        for (int i = 0; i < 10; i++) {
            try {
                Response response = ClientBuilder.newClient()
                        .target("http://localhost:8070/lra-coordinator")
                        .path("recovery")
                        .request()
                        .async()
                        .get()
                        .get(2, TimeUnit.SECONDS);

                String recoveringLras = response.readEntity(String.class);
                // response.close();
                if (!recoveringLras.contains(lraId.toASCIIString())) {
                    // intended LRA is not longer among those recovering
                    break;
                }
                System.out.println("Waiting for recovery attempt #" + i + " LRA is still waiting: " + recoveringLras);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                // timeout can be expected, lets try again
            }
        }
    }
}
