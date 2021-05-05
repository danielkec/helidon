/*
 * Copyright (c) 2021 Oracle and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import io.helidon.common.configurable.ScheduledThreadPoolSupplier;
import io.helidon.common.reactive.Single;
import io.helidon.microprofile.config.ConfigCdiExtension;
import io.helidon.microprofile.lra.coordinator.Coordinator;
import io.helidon.microprofile.lra.coordinator.CoordinatorApplication;
import io.helidon.microprofile.lra.coordinator.client.CoordinatorClient;
import io.helidon.microprofile.lra.coordinator.client.NarayanaClient;
import io.helidon.microprofile.lra.coordinator.client.NarayanaResourceAdapter;
import io.helidon.microprofile.lra.resources.CdiCompleteOrCompensate;
import io.helidon.microprofile.lra.resources.CommonAfter;
import io.helidon.microprofile.lra.resources.DontEnd;
import io.helidon.microprofile.lra.resources.JaxrsCompleteOrCompensate;
import io.helidon.microprofile.lra.resources.NestedCompleteOrCompensate;
import io.helidon.microprofile.lra.resources.Recovery;
import io.helidon.microprofile.lra.resources.RecoveryStatus;
import io.helidon.microprofile.lra.resources.StartAndAfter;
import io.helidon.microprofile.lra.resources.TestApplication;
import io.helidon.microprofile.lra.resources.Timeout;
import io.helidon.microprofile.lra.resources.Work;
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
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@HelidonTest
@DisableDiscovery
// Helidon MP
@AddExtension(ConfigCdiExtension.class)
@AddExtension(ServerCdiExtension.class)
@AddExtension(JaxRsCdiExtension.class)
@AddExtension(CdiComponentProvider.class)
// LRA client
@AddExtension(LRACdiExtension.class)
@AddBean(InspectionService.class)
@AddBean(ParticipantService.class)
@AddBean(ParticipantApp.class)
@AddConfig(key = CoordinatorClient.CONF_KEY_COORDINATOR_URL, value = "http://localhost:8070/lra-coordinator")
// Coordinator flavor
@AddBean(NarayanaClient.class)
@AddBean(NarayanaResourceAdapter.class)
// Test resources
@AddBean(TestApplication.class)
@AddBean(JaxrsCompleteOrCompensate.class)
@AddBean(CdiCompleteOrCompensate.class)
@AddBean(StartAndAfter.class)
@AddBean(DontEnd.class)
@AddBean(Timeout.class)
@AddBean(Recovery.class)
@AddBean(RecoveryStatus.class)
@AddBean(NestedCompleteOrCompensate.class)
// Mock coordinator
// comment out below annotations to use external coordinator
@AddBean(Coordinator.class)
@AddBean(CoordinatorApplication.class)
@AddExtension(SchedulingCdiExtension.class)
@AddConfig(key = "io.helidon.microprofile.lra.coordinator.CoordinatorApplication."
        + RoutingName.CONFIG_KEY_NAME, value = "coordinator")
@AddConfig(key = "server.sockets.0.name", value = "coordinator")
@AddConfig(key = "server.sockets.0.port", value = "8070")
@AddConfig(key = "server.sockets.0.bind-address", value = "localhost")
public class BasicTest {

    private static final long TIMEOUT_SEC = 10L;

    private static ScheduledExecutorService executor;
    private final Map<String, CompletableFuture<URI>> completionMap = new HashMap<>();

    @SuppressWarnings("unchecked")
    public synchronized <T> CompletableFuture<T> getCompletable(String key, URI lraId) {
        String combinedKey = key + Optional.ofNullable(lraId).map(URI::toASCIIString).orElse("");
        completionMap.putIfAbsent(combinedKey, new CompletableFuture<>());
        return (CompletableFuture<T>) completionMap.get(combinedKey);
    }

    public synchronized <T> CompletableFuture<T> getCompletable(String key) {
        return getCompletable(key, null);
    }

    public <T> T await(String key, URI lraId) {
        return Single.<T>create(getCompletable(key, lraId), true).await(TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    public <T> T await(String key) {
        return Single.<T>create(getCompletable(key), true).await(TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    @Inject
    CoordinatorClient coordinatorClient;

    @BeforeAll
    static void beforeAll() {
        executor = ScheduledThreadPoolSupplier.create().get();
    }

    @BeforeEach
    void setUp() {
        completionMap.clear();
    }

    @AfterAll
    static void afterAll() {
        executor.shutdownNow();
    }

    @Test
    void jaxrsComplete(WebTarget target) throws Exception {
        Response response = target.path(JaxrsCompleteOrCompensate.PATH_BASE)
                .path(JaxrsCompleteOrCompensate.PATH_START_LRA)
                .request()
                .header(Work.HEADER_KEY, Work.NOOP)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), AnyOf.anyOf(is(200), is(204)));
        URI lraId = await(JaxrsCompleteOrCompensate.CS_START_LRA);
        assertThat(await(JaxrsCompleteOrCompensate.CS_COMPLETE), is(lraId));
        assertFalse(getCompletable(JaxrsCompleteOrCompensate.CS_COMPENSATE).isDone());
    }

    @Test
    void jaxrsCompensate(WebTarget target) throws Exception {
        Response response = target.path(JaxrsCompleteOrCompensate.PATH_BASE)
                .path(JaxrsCompleteOrCompensate.PATH_START_LRA)
                .request()
                .header(Work.HEADER_KEY, Work.BOOM)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(500));
        URI lraId = await(JaxrsCompleteOrCompensate.CS_START_LRA);
        assertThat(await(JaxrsCompleteOrCompensate.CS_COMPENSATE), is(lraId));
        assertFalse(getCompletable(JaxrsCompleteOrCompensate.CS_COMPLETE).isDone());
    }

    @Test
    void cdiComplete(WebTarget target) throws Exception {
        Response response = target.path(CdiCompleteOrCompensate.PATH_BASE)
                .path(CdiCompleteOrCompensate.PATH_START_LRA)
                .request()
                .header(Work.HEADER_KEY, Work.NOOP)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), AnyOf.anyOf(is(200), is(204)));
        URI lraId = await(CdiCompleteOrCompensate.CS_START_LRA);
        assertThat(await(CdiCompleteOrCompensate.CS_COMPLETE), is(lraId));
        assertFalse(getCompletable(CdiCompleteOrCompensate.CS_COMPENSATE).isDone());
    }

    @Test
    void cdiCompensate(WebTarget target) throws Exception {
        Response response = target.path(CdiCompleteOrCompensate.PATH_BASE)
                .path(CdiCompleteOrCompensate.PATH_START_LRA)
                .request()
                .header(Work.HEADER_KEY, Work.BOOM)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(500));
        URI lraId = await(CdiCompleteOrCompensate.CS_START_LRA);
        assertThat(await(CdiCompleteOrCompensate.CS_COMPENSATE), is(lraId));
        assertFalse(getCompletable(CdiCompleteOrCompensate.CS_COMPLETE).isDone());
    }

    @Test
    void nestedComplete(WebTarget target) throws Exception {
        // Start parent LRA
        Response response = target.path(NestedCompleteOrCompensate.PATH_BASE)
                .path(NestedCompleteOrCompensate.PATH_START_PARENT_LRA)
                .request()
                .header(Work.HEADER_KEY, Work.NOOP)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);

        assertThat(response.getStatus(), is(200));
        URI parentLraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(await(NestedCompleteOrCompensate.CS_START_PARENT_LRA), is(parentLraId));

        // Start nested LRA
        response = target.path(NestedCompleteOrCompensate.PATH_BASE)
                .path(NestedCompleteOrCompensate.PATH_START_NESTED_LRA)
                .request()
                .header(LRA_HTTP_CONTEXT_HEADER, parentLraId)
                .header(Work.HEADER_KEY, Work.NOOP)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);

        assertThat(response.getStatus(), is(200));
        URI nestedLraId = await(NestedCompleteOrCompensate.CS_START_NESTED_LRA);

        // Nothing is ended, completed or compensated
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_END_PARENT_LRA).isDone());
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_END_NESTED_LRA).isDone());
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_COMPENSATED, parentLraId).isDone());
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_COMPENSATED, nestedLraId).isDone());
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_COMPLETED, parentLraId).isDone());
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_COMPLETED, nestedLraId).isDone());

        // End nested LRA
        response = target.path(NestedCompleteOrCompensate.PATH_BASE)
                .path(NestedCompleteOrCompensate.PATH_END_NESTED_LRA)
                .request()
                .header(LRA_HTTP_CONTEXT_HEADER, nestedLraId)
                .header(Work.HEADER_KEY, Work.NOOP)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);

        assertThat(response.getStatusInfo().getReasonPhrase(), response.getStatus(), is(200));
        assertThat(UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build(), is(nestedLraId));
        assertThat(await(NestedCompleteOrCompensate.CS_END_NESTED_LRA), is(nestedLraId));
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_END_PARENT_LRA).isDone());
        assertThat("Nested LRA should have completed and get parent LRA in the header.",
                await(NestedCompleteOrCompensate.CS_COMPLETED, nestedLraId), is(parentLraId));

        // End parent LRA
        response = target.path(NestedCompleteOrCompensate.PATH_BASE)
                .path(NestedCompleteOrCompensate.PATH_END_PARENT_LRA)
                .request()
                .header(LRA_HTTP_CONTEXT_HEADER, parentLraId)
                .header(Work.HEADER_KEY, Work.NOOP)
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);

        assertThat(response.getStatusInfo().getReasonPhrase(), response.getStatus(), is(200));
        assertThat(UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build(), is(parentLraId));
        assertThat(await(NestedCompleteOrCompensate.CS_END_PARENT_LRA), is(parentLraId));

        assertThat("Parent LRA should have completed and get null as parent LRA in the header.",
                await(NestedCompleteOrCompensate.CS_COMPLETED, parentLraId), is(IsNull.nullValue()));

        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_COMPENSATED, parentLraId).isDone());
        assertFalse(getCompletable(NestedCompleteOrCompensate.CS_COMPENSATED, nestedLraId).isDone());
    }

    @Test
    void startAndAfter(WebTarget target) throws Exception {
        Response response = target.path(StartAndAfter.PATH_BASE)
                .path(StartAndAfter.PATH_START_LRA)
                .request()
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), AnyOf.anyOf(is(200), is(204)));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(await(StartAndAfter.CS_START_LRA), is(lraId));
        await(CommonAfter.CS_AFTER, lraId);
    }

    @Test
    void firstNotEnding(WebTarget target) throws Exception {
        Response response = target.path(DontEnd.PATH_BASE)
                .path(DontEnd.PATH_START_LRA)
                .request()
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), AnyOf.anyOf(is(200), is(204)));
        URI lraId = await(DontEnd.CS_START_LRA);
        assertThat(coordinatorClient.status(lraId), is(LRAStatus.Active));
        assertThat(target.path(DontEnd.PATH_BASE)
                .path(DontEnd.PATH_START_SECOND_LRA)
                .request()
                .header(LRA_HTTP_CONTEXT_HEADER, lraId)
                .async()
                .put(Entity.text(""))
                .get(10, TimeUnit.SECONDS).getStatus(), AnyOf.anyOf(is(200), is(204)));
        await(DontEnd.CS_START_SECOND_LRA);
        assertClosedOrNotFound(lraId);
    }

    @Test
    void timeout(WebTarget target) throws Exception {
        Response response = target.path(Timeout.PATH_BASE)
                .path(Timeout.PATH_START_LRA)
                .request()
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(200));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(await(Timeout.CS_START_LRA), is(lraId));
        assertThat(await(Timeout.CS_COMPENSATE), is(lraId));
    }

    @Test
    void compensateRecoveryTest(WebTarget target) throws ExecutionException, InterruptedException, TimeoutException {
        LocalDateTime start = LocalDateTime.now();
        Response response = target.path(Recovery.PATH_BASE)
                .path(Recovery.PATH_START_COMPENSATE_LRA)
                .request()
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(500));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(await(Recovery.CS_START_COMPENSATE_LRA), is(lraId));
        assertThat(await(Recovery.CS_COMPENSATE_FIRST), is(lraId));
        LocalDateTime first = LocalDateTime.now();
        System.out.println("First compensate attempt after " + Duration.between(start, first));
        waitForRecovery(lraId);
        assertThat(await(Recovery.CS_COMPENSATE_SECOND), is(lraId));
        LocalDateTime second = LocalDateTime.now();
        System.out.println("Second compensate attempt after " + Duration.between(first, second));
    }

    @Test
    void completeRecoveryTest(WebTarget target) throws ExecutionException, InterruptedException, TimeoutException {
        LocalDateTime start = LocalDateTime.now();
        Response response = target.path(Recovery.PATH_BASE)
                .path(Recovery.PATH_START_COMPLETE_LRA)
                .request()
                .async()
                .put(Entity.text(""))
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertThat(response.getStatus(), is(200));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(await(Recovery.CS_START_COMPLETE_LRA), is(lraId));
        assertThat(await(Recovery.CS_COMPLETE_FIRST), is(lraId));
        LocalDateTime first = LocalDateTime.now();
        System.out.println("First complete attempt after " + Duration.between(start, first));
        waitForRecovery(lraId);
        assertThat(await(Recovery.CS_COMPLETE_SECOND), is(lraId));
        LocalDateTime second = LocalDateTime.now();
        System.out.println("Second complete attempt after " + Duration.between(first, second));
    }

    @Test
    void statusRecoveryTest(WebTarget target) throws ExecutionException, InterruptedException, TimeoutException {

        Response response = target.path(RecoveryStatus.PATH_BASE)
                .path(RecoveryStatus.PATH_START_LRA)
                .request()
                .async()
                .put(Entity.text(ParticipantStatus.Active.name()))// report from @Status method
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);

        assertThat(response.getStatus(), is(500));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(await(RecoveryStatus.CS_START_LRA, lraId), is(lraId));
        waitForRecovery(lraId);
        assertThat(await(RecoveryStatus.CS_STATUS, lraId), is(lraId));
        assertThat(await(RecoveryStatus.CS_COMPENSATE_SECOND, lraId), is(lraId));
    }

    @Test
    void statusNonRecoveryTest(WebTarget target) throws ExecutionException, InterruptedException, TimeoutException {

        Response response = target.path(RecoveryStatus.PATH_BASE)
                .path(RecoveryStatus.PATH_START_LRA)
                .request()
                .async()
                .put(Entity.text(ParticipantStatus.Compensated.name()))// report from @Status method
                .get(TIMEOUT_SEC, TimeUnit.SECONDS);

        assertThat(response.getStatus(), is(500));
        URI lraId = UriBuilder.fromPath(response.getHeaderString(LRA_HTTP_CONTEXT_HEADER)).build();
        assertThat(await(RecoveryStatus.CS_START_LRA, lraId), is(lraId));
        waitForRecovery(UriBuilder.fromPath("fake_non_existent").build());// just wait for any recovery
        waitForRecovery(UriBuilder.fromPath("fake_non_existent").build());// just wait for any recovery
        assertThat("@Status method should have been called by compensator",
                await(RecoveryStatus.CS_STATUS, lraId), is(lraId));
        assertThat("Second compensation shouldn't come, we reported Completed with @Status method",
                getCompletable(RecoveryStatus.CS_COMPENSATE_SECOND, lraId).isDone(), is(not(true)));
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
            Client client = ClientBuilder.newClient();
            try {
                Response response = client
                        .target("http://localhost:8070/lra-coordinator")
                        .path("recovery")
                        .request()
                        .get();

                String recoveringLras = response.readEntity(String.class);
                response.close();
                if (!recoveringLras.contains(lraId.toASCIIString())) {
                    System.out.println("LRA is no longer among those recovering " + lraId.toASCIIString());
                    // intended LRA is not longer among those recovering
                    break;
                }
                System.out.println("Waiting for recovery attempt #" + i + " LRA is still waiting: " + recoveringLras);
            } finally {
                client.close();
            }
        }
    }
}
