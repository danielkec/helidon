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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import static javax.ws.rs.core.Response.Status.GONE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.lra.annotation.LRAStatus;

@ApplicationScoped
public class NarayanaClient implements CoordinatorClient {

    private final String coordinatorUrl = "http://localhost:8070/lra-coordinator";

    public NarayanaClient() {

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
                throw new WebApplicationException("Unexpected response " + response.getStatus() + " from coordinator "
                        + (response.hasEntity() ? response.readEntity(String.class) : ""));
            }
            return UriBuilder.fromPath("LRAID" + response.getHeaderString(HttpHeaders.LOCATION).split("LRAID")[1]).build();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new WebApplicationException("Unable to start LRA", e);
        }
    }

    @Override
    public void cancel(final URI lraId) throws WebApplicationException {
        throw new RuntimeException("Not implemented yet!!!");
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
                    throw new NotFoundException("NOTFOUND", Response.status(NOT_FOUND).entity(lraId.toASCIIString()).build());
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
    public URI join(URI lraId,
                    Long timeLimit,
                    URI compensate,
                    URI complete,
                    URI forget,
                    URI leave,
                    URI after,
                    URI status,
                    String compensatorData) throws WebApplicationException {
        try {
            String links = Map.of("compensate", Optional.ofNullable(compensate),
                    "complete", Optional.ofNullable(complete),
                    "forget", Optional.ofNullable(forget),
                    "leave", Optional.ofNullable(leave),
                    "after", Optional.ofNullable(after),
                    "status", Optional.ofNullable(status))
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
    public URI join(final URI lraId, final Long timeLimit, final URI participant, final String compensatorData) throws
            WebApplicationException {
        throw new RuntimeException("Not implemented yet!!!");
    }

    @Override
    public void leave(final URI lraId, final String body) throws WebApplicationException {
        throw new RuntimeException("Not implemented yet!!!");
    }

    @Override
    public LRAStatus getStatus(final URI uri) throws WebApplicationException {
        throw new RuntimeException("Not implemented yet!!!");
    }
}
