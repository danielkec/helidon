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
package io.helidon.microprofile.lra.coordinator.client;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import static javax.ws.rs.core.Response.Status.GONE;
import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

@ApplicationScoped
@Named("narayana")
public class NarayanaClient implements CoordinatorClient {

    private final String coordinatorUrl;

    private static final Logger LOGGER = Logger.getLogger(NarayanaClient.class.getName());

    @Inject
    public NarayanaClient(@ConfigProperty(name = CoordinatorClient.CONF_KEY_COORDINATOR_URL) String coordinatorUrl) {
        this.coordinatorUrl = coordinatorUrl;
    }

    @Override
    public URI start(final URI parentLRA, final String clientID, final Long timeout) throws WebApplicationException {
        try {
            Response response = ClientBuilder.newClient()
                    .target(coordinatorUrl)
                    .path("start")
                    .queryParam("ClientID", Optional.ofNullable(clientID).orElse(""))
                    .queryParam("TimeLimit", Optional.ofNullable(timeout).orElse(0L))
                    .queryParam("ParentLRA", Optional.ofNullable(parentLRA)
                            .map(p -> URLEncoder.encode(p.toString(), StandardCharsets.UTF_8))
                            .orElse(""))
                    .request()
                    .async()
                    .post(null)
                    .get(10, TimeUnit.SECONDS);
            if (response.getStatus() != 201) {
                LOGGER.log(Level.SEVERE, "Unexpected response from coordinator. " + response.getStatusInfo().getReasonPhrase());
                throw new WebApplicationException("Unexpected response " + response.getStatus() + " from coordinator "
                        + (response.hasEntity() ? response.readEntity(String.class) : ""));
            }
            return NarayanaLRAId.parseLRAId(response.getHeaderString(HttpHeaders.LOCATION));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new WebApplicationException("Unable to start LRA", e);
        }
    }

    @Override
    public void cancel(final URI lraId) throws WebApplicationException {
        try {
            Response response = ClientBuilder.newClient()
                    .target(coordinatorUrl)
                    .path(lraId.toASCIIString())
                    .path("cancel")
                    .request()
                    .async()
                    .put(Entity.text(""))
                    .get(10, TimeUnit.SECONDS);

            switch (response.getStatus()) {
                case 404:
                    LOGGER.warning("Cancel LRA - Coordinator can't find LRAID: " + lraId.toASCIIString());
                case 200:
                case 202:
                    break;
                default:
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new WebApplicationException("Unable to start LRA", e);
        }
    }

    @Override
    public void close(final URI lraId) throws WebApplicationException {
        try {
            Response response = ClientBuilder.newClient()
                    .target(coordinatorUrl)
                    .path(lraId.toASCIIString())
                    .path("close")
                    .request()
                    .async()
                    .put(Entity.text(""))
                    .get(10, TimeUnit.SECONDS);

            switch (response.getStatus()) {
                case 404:
                    LOGGER.warning("Closing LRA - Coordinator can't find LRAID: " + lraId.toASCIIString());
                case 200:
                case 202:
                    break;
                default:
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new WebApplicationException("Unable to close LRA", e);
        }
    }

    @Override
    public URI join(URI lraId,
                    Long timeLimit,
                    Participant participant) throws WebApplicationException {
        try {
            String links = compensatorLinks(participant);
            LOGGER.log(Level.INFO, "JOIN LINKS: " + links);
            Response response = ClientBuilder.newClient()
                    .target(coordinatorUrl)
                    .path(lraId.toASCIIString())
                    .queryParam("TimeLimit", timeLimit)
                    .request()
                    .header("Link", links)
                    .async()
                    .put(Entity.text(links))
                    .get(10, TimeUnit.SECONDS);

            switch (response.getStatus()) {
                case 412:
                    throw new WebApplicationException("Too late to join LRA " + lraId,
                            Response.status(PRECONDITION_FAILED).entity("Too late to join LRA " + lraId).build());
                case 404:
                    throw new WebApplicationException("Not found " + lraId,
                            Response.status(GONE).entity("Not found " + lraId).build());
                case 200:
                    String recoveryHeader = response.getHeaderString(LRA_HTTP_RECOVERY_HEADER);
                    if (recoveryHeader != null && !recoveryHeader.isEmpty()) {
                        return UriBuilder.fromPath(recoveryHeader).build();
                    }
                    return null;
                default:
                    throw new WebApplicationException("Can't join LRA " + lraId, response.getStatus());
            }

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new WebApplicationException("Unable to join LRA " + lraId, e);
        }
    }

    @Override
    public void leave(URI lraId, Participant participant) throws WebApplicationException {
        try {
            Response response = ClientBuilder.newClient()
                    .target(coordinatorUrl)
                    .path(lraId.toASCIIString())
                    .path("remove")
                    .request()
                    .async()
                    .put(Entity.text(compensatorLinks(participant)))
                    .get(10, TimeUnit.SECONDS);

            switch (response.getStatus()) {
                case 404:
                    LOGGER.warning("Leaving LRA - Coordinator can't find LRAID: " + lraId.toASCIIString());
                case 200:
                    LOGGER.log(Level.INFO, "Left LRA - " + lraId.toASCIIString());
                    return;
                default:
                    throw new IllegalStateException("Unexpected coordinator response " + response.getStatus());
            }

        } catch (InterruptedException | ExecutionException | TimeoutException | IllegalStateException e) {
            throw new WebApplicationException("Unable to leave LRA " + lraId, e);
        }
    }


    @Override
    public LRAStatus status(final URI lraId) throws WebApplicationException {
        try {
            Response response = ClientBuilder.newClient()
                    .target(coordinatorUrl)
                    .path(lraId.toASCIIString())
                    .path("status")
                    .request()
                    .async()
                    .get()
                    .get(10, TimeUnit.SECONDS);

            switch (response.getStatus()) {
                case 404:
                    LOGGER.warning("Status LRA - Coordinator can't find LRAID: " + lraId.toASCIIString());
                    return LRAStatus.Closed;
                case 200:
                case 202:
                    return response.readEntity(LRAStatus.class);
                default:
                    throw new IllegalStateException("Unexpected coordinator response " + response.getStatus());
            }

        } catch (InterruptedException | ExecutionException | TimeoutException | IllegalStateException e) {
            throw new WebApplicationException("Unable to retrieve status of LRA " + lraId, e);
        }
    }

    private String compensatorLinks(Participant p) {
        return Map.of(
                "compensate", p.compensate(),
                "complete", p.complete(),
                "forget", p.forget(),
                "leave", p.leave(),
                "after", p.after(),
                "status", p.status()
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
}

