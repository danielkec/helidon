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
package io.helidon.lra.rest;

import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.jboss.jandex.*;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.DeploymentException;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.WithAnnotations;
import javax.enterprise.util.AnnotationLiteral;
import javax.ws.rs.Path;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static javax.interceptor.Interceptor.Priority.PLATFORM_AFTER;

public class LRACdiExtension implements Extension {

    private ClassPathIndexer classPathIndexer = new ClassPathIndexer();
    private Index index;
    private final Map<String, LRAParticipant> participants = new HashMap<>();

    private void registerChannelMethods(
            @Observes
            @WithAnnotations({
                    Path.class,
                    AfterLRA.class,
                    Compensate.class,
                    Complete.class,
                    Forget.class,
                    Status.class
            }) ProcessAnnotatedType<?> pat) {
        // Lookup channel methods
        LRAParticipant participant = getAsParticipant(pat.getAnnotatedType());
        if (participant != null) {
            participants.put(participant.getJavaClass().getName(), participant);
        }
    }

    public void observe(@Observes AfterBeanDiscovery event, BeanManager beanManager) {
        for (LRAParticipant participant : participants.values()) {
            Set<Bean<?>> participantBeans = beanManager.getBeans(participant.getJavaClass(), new AnnotationLiteral<Any>() {});
            if (participantBeans.isEmpty()) {
                // resource is not registered as managed bean so register a custom managed instance
                try {
                    participant.setInstance(participant.getJavaClass().newInstance());
                } catch (InstantiationException | IllegalAccessException e) {
                    // todo LRALogger.i18NLogger.error_cannotProcessParticipant(e);
                }
            }
        }
}


    private void makeConnections(@Observes @Priority(PLATFORM_AFTER + 101) @Initialized(ApplicationScoped.class) Object event,
                                 BeanManager beanManager) {
        Bean<?> participantRegistryBean = beanManager.resolve(beanManager.getBeans(LRAParticipantRegistry.class));
        LRAParticipantRegistry registry = lookup(participantRegistryBean, beanManager);

        registry.lraParticipants().putAll(participants);
    }

    /**
     * Collects all non-JAX-RS participant methods in the defined Java class
     *
     * @param classInfo a Jandex class info of the class to be scanned
     * @return Collected methods wrapped in {@link LRAParticipant} class or null if no non-JAX-RS methods have been found
     */
    private LRAParticipant getAsParticipant(AnnotatedType<?> classInfo) {
        Class<?> javaClass = classInfo.getJavaClass();

        if (javaClass.isInterface() || Modifier.isAbstract(javaClass.getModifiers()) || !isLRAParticipant(classInfo)) {
            return null;
        }

        LRAParticipant participant = new LRAParticipant(javaClass);
        return participant.hasNonJaxRsMethods() ? participant : null;
    }

    /**
     * Returns whether the classinfo represents an LRA participant --
     * Class contains LRA method and either one or both of Compensate and/or AfterLRA methods.
     *
     * @param classInfo Jandex class object to scan for annotations
     * @return true if the class is a valid LRA participant, false otherwise
     * @throws IllegalStateException if there is LRA annotation but no Compensate or AfterLRA is found
     */
    private boolean isLRAParticipant(AnnotatedType<?> classInfo) {
        //Map<DotName, List<AnnotationInstance>> annotations = JandexAnnotationResolver.getAllAnnotationsFromClassInfoHierarchy(classInfo.name(), index);

        Set<Class<?>> annotations = classInfo.getMethods().stream()
                .flatMap(am -> am.getAnnotations().stream())
                .map(Annotation::annotationType)
                .collect(Collectors.toSet());

        if (!annotations.contains(LRA.class)) {
            return false;
        } else if (!annotations.contains(Compensate.class) && !annotations.contains(AfterLRA.class)) {
            throw new IllegalStateException(String.format("%s: %s",
                    classInfo.getJavaClass().getName(), "The class contains an LRA method and no Compensate or AfterLRA method was found."));
        } else {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    static <T> T lookup(Bean<?> bean, BeanManager beanManager) {
        javax.enterprise.context.spi.Context context = beanManager.getContext(bean.getScope());
        Object instance = context.get(bean);
        if (instance == null) {
            CreationalContext<?> creationalContext = beanManager.createCreationalContext(bean);
            instance = beanManager.getReference(bean, bean.getBeanClass(), creationalContext);
        }
        if (instance == null) {
            throw new DeploymentException("Instance of bean " + bean.getName() + " not found");
        }
        return (T) instance;
    }
}
