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
import javax.inject.Inject;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import io.helidon.microprofile.lra.BasicTest;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

@ApplicationScoped
@Path("/dont-end")
public class DontEnd extends CommonAfter {

    @Inject
    BasicTest basicTest;

    @PUT
    @Path("first-not-ending")
    @LRA(value = LRA.Type.REQUIRES_NEW, end = false)
    public Response startDontEndLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
        basicTest.getCompletable("first-not-ending").complete(lraId);
        return Response.ok().build();
    }

    @PUT
    @Path("second-ending")
    @LRA(value = LRA.Type.MANDATORY, end = true)
    public Response endLRA(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId) {
        basicTest.getCompletable("second-ending").complete(lraId);
        return Response.ok().build();
    }

}
