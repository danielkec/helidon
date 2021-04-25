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

package io.helidon.microprofile.lra.tck;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.eclipse.microprofile.lra.tck.service.spi.LRACallbackException;
import org.eclipse.microprofile.lra.tck.service.spi.LRARecoveryService;

public class HelidonLRARecoveryService implements LRARecoveryService {

    private static final Logger LOGGER = Logger.getLogger(HelidonLRARecoveryService.class.getName());

    @Override
    public void waitForCallbacks(URI lraId) {
    }

    @Override
    public void waitForRecovery(URI lraId) throws LRACallbackException {
        int counter = 0;

        do {
            if (counter > 1) return;
            LOGGER.info("Recovery attempt #" + ++counter);
        } while (!waitForEndPhaseReplay(lraId));
        LOGGER.info("LRA " + lraId + "has finished the recovery");
    }

    @Override
    public boolean waitForEndPhaseReplay(URI lraId) {
        try {
            Response response = ClientBuilder.newClient()
                    .target("http://localhost:8070/lra-coordinator")
                    .path("recovery")
                    .request()
                    .async()
                    .get()
                    .get(2, TimeUnit.SECONDS);

            String recoveringLras = response.readEntity(String.class);
            if (recoveringLras.contains(lraId.toASCIIString())) {
                // intended LRA is among those still waiting for recovering
                return false;
            }
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            // timeout can be expected, lets try again
            return false;
        }
        return true;
    }
}