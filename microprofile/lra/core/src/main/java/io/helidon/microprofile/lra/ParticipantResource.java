package io.helidon.microprofile.lra;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import org.eclipse.microprofile.lra.LRAResponse;

@ApplicationScoped
@Path("lra-client-cdi-methods")
public class ParticipantResource {
    //http://127.0.0.1:43733/lra-client-cdi-methods/complete/io.helidon.microprofile.lra.TestApplication$StartAndCloseCdi/complete
    @Inject
    private ParticipantService participantService;

    @PUT
    @Path("/compensate/{fqdn}/{methodName}")
    //@Produces(MediaType.APPLICATION_JSON)
    public Response compensate(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                               @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId,
                               @PathParam("fqdn") String fqdn,
                               @PathParam("methodName") String methodName) {
        try {
            return participantService.invoke(fqdn, methodName, lraId, recoveryId);
        } catch (InvocationTargetException e) {
            return LRAResponse.completed();
        }
    }

    @PUT
    @Path("/complete/{fqdn}/{methodName}")
    //@Produces(MediaType.APPLICATION_JSON)
    public Response complete(@HeaderParam(LRA_HTTP_CONTEXT_HEADER) URI lraId,
                             @HeaderParam(LRA_HTTP_RECOVERY_HEADER) URI recoveryId,
                             @PathParam("fqdn") String fqdn,
                             @PathParam("methodName") String methodName) {
        try {
            return participantService.invoke(fqdn, methodName, lraId, recoveryId);
        } catch (InvocationTargetException e) {
            return LRAResponse.completed();
        }
    }

}
