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

package io.helidon.microprofile.lra.resources;

import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

import java.net.URI;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

@ApplicationScoped
@Path("/start-and-after")
public class StartAndAfter extends CommonAfter {

    @PUT
    @LRA(LRA.Type.REQUIRES_NEW)
    @Path("/start")
    public void doInTransaction(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
        //Single.timer(100, TimeUnit.MILLISECONDS, executor).await();
    }

}
