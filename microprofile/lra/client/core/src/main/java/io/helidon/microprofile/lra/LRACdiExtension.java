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

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AnnotatedParameter;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.DeploymentException;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessManagedBean;
import javax.enterprise.inject.spi.WithAnnotations;
import javax.inject.Named;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import io.helidon.config.Config;
import io.helidon.microprofile.lra.coordinator.client.CoordinatorClient;

import static javax.interceptor.Interceptor.Priority.PLATFORM_AFTER;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.CompositeIndex;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexReader;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;
import org.jboss.jandex.MethodInfo;

public class LRACdiExtension implements Extension {

    private static final Logger LOGGER = Logger.getLogger(LRACdiExtension.class.getName());
    private final Config config = (Config) ConfigProvider.getConfig();

    private final Indexer indexer;
    private final String configuredCoordinatorClient;
    private IndexView index = null;
    private final ClassLoader classLoader;

    public LRACdiExtension() {
        indexer = new Indexer();
        classLoader = Thread.currentThread().getContextClassLoader();
        configuredCoordinatorClient = config.get("mp.lra.coordinator.client").as(String.class).orElse(null);
        // Needs to be always indexed
        Set.of(LRA.class,
                AfterLRA.class,
                Compensate.class,
                Complete.class,
                Forget.class,
                Status.class,
                ParticipantCdiResource.class).forEach(c -> runtimeIndex(DotName.createSimple(c.getName())));

        List<URL> indexFiles;
        try {
            indexFiles = findIndexFiles("META-INF/jandex.idx");
            if (!indexFiles.isEmpty()) {
                index = CompositeIndex.create(indexer.complete(), existingIndexFileReader(indexFiles));
            } else {
                index = null;
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error when locating Jandex index, fall-back to runtime computed index.", e);
            index = null;
        }
    }

    private void index(
            @Observes
            @WithAnnotations({
                    Path.class,
                    GET.class, POST.class, PUT.class, DELETE.class,
                    LRA.class,
                    AfterLRA.class, Compensate.class, Complete.class, Forget.class, Status.class
            }) ProcessAnnotatedType<?> pat) {
        // compile time bilt index 
        if (index != null) return;
        // create runtime index when pre-built index is not available
        runtimeIndex(DotName.createSimple(pat.getAnnotatedType().getJavaClass().getName()));
    }

    private void validate(@Observes
                          @WithAnnotations(
                                  {
                                          Complete.class,
                                          Compensate.class,
                                          Forget.class,
                                          AfterLRA.class
                                  })
                                  ProcessAnnotatedType<?> pat) {
        Set<Class<? extends Annotation>> expected = Set.of(AfterLRA.class, Complete.class, Compensate.class, Forget.class);
        Set<Class<? extends Annotation>> excluded = Set.of(PUT.class, Path.class);
        AnnotatedType<?> annotatedType = pat.getAnnotatedType();
        annotatedType.getMethods().stream()
                .filter(m -> m.getAnnotations().stream().map(Annotation::annotationType).anyMatch(expected::contains))
                .filter(m -> m.getAnnotations().stream().map(Annotation::annotationType).noneMatch(excluded::contains))
                .forEach(m -> {
                    List<? extends AnnotatedParameter<?>> parameters = m.getParameters();
                    if (parameters.size() > 2) {
                        throw new DeploymentException("Too many arguments on compensate method " + m.getJavaMember());
                    }
                    if (parameters.size() == 1 && !parameters.get(0).getBaseType().equals(URI.class)) {
                        throw new DeploymentException("First argument of LRA method " + m.getJavaMember() + " must be of type URI");
                    }
                    if (parameters.size() == 2) {
                        if (!parameters.get(0).getBaseType().equals(URI.class)) {
                            throw new DeploymentException("First argument of LRA method " + m.getJavaMember() + " must be of type URI");
                        }
                        if (!Set.of(URI.class, LRAStatus.class).contains(parameters.get(1).getBaseType())) {
                            throw new DeploymentException("Second argument of LRA method " + m.getJavaMember() + " must be of type URI");
                        }
                    }
                });
    }

    public void chooseCoordinatorClient(@Observes ProcessAnnotatedType<CoordinatorClient> pat) {
        Class<CoordinatorClient> clazz = pat.getAnnotatedType().getJavaClass();
        if (CoordinatorClient.class.isAssignableFrom(clazz)) {
            // Exclude coordinators only when there is configured specific one
            if (configuredCoordinatorClient != null) {
                String name = Optional.ofNullable(pat.getAnnotatedType().getAnnotation(Named.class))
                        .map((Function<Named, Object>) Named::value)
                        .map(String::valueOf)
                        // No @Named annotation, default with class simple name
                        .orElseGet(clazz::getSimpleName);
                if (!configuredCoordinatorClient.equals(name)) {
                    // There is configured different client name, skip this one
                    LOGGER.log(Level.WARNING, "Excluding coordinator client {0}, configured client is {1}",
                            new Object[] {name, configuredCoordinatorClient});
                    pat.veto();
                }
            }
        }
    }


    private final Map<Class<?>, Bean<?>> lraCdiBeanReferences = new HashMap<>();

    private void cdiLRABeanReferences(@Observes ProcessManagedBean<?> event) {
        //TODO: limit only to beans with Complete/Compensate/Status lra cdi methods
        lraCdiBeanReferences.put(event.getBean().getBeanClass(), event.getBean());
    }

    private void ready(
            @Observes
            @Priority(PLATFORM_AFTER + 101)
            @Initialized(ApplicationScoped.class) Object event,
            BeanManager beanManager) {

        if (index == null) {
            // compile time built index 
            index = indexer.complete();
        }

        // ------------- Validate LRA methods ------------- 
        // TODO: Clean up and externalize
        InspectionService inspectionService =
                lookup(beanManager.resolve(beanManager.getBeans(InspectionService.class)), beanManager);

        for (ClassInfo classInfo : index.getKnownClasses()) {

            if (Modifier.isInterface(classInfo.flags()) || Modifier.isAbstract(classInfo.flags())) {
                // skip
                continue;
            }

            Map<MethodInfo, Set<AnnotationInstance>> lraMethods = inspectionService.lookUpLraMethods(classInfo);

            if (lraMethods.isEmpty()) {
                // no lra methods
                continue;
            }


            // one of compensate or afterLra is mandatory
            Set<DotName> mandatoryAnnotations = Set.of(InspectionService.COMPENSATE, InspectionService.AFTER_LRA);
            if (lraMethods.values().stream()
                    .flatMap(Set::stream)
                    .map(AnnotationInstance::name)
                    .noneMatch(mandatoryAnnotations::contains)) {
                throw new DeploymentException("Missing  @Compensate or @AfterLRA on class " + classInfo);
            }


            //allowed return types for compensate and afterLra
            Set<DotName> returnTypes = Set.of(
                    Response.class,
                    ParticipantStatus.class,
                    CompletionStage.class,
                    void.class
            ).stream()
                    .map(Class::getName)
                    .map(DotName::createSimple)
                    .collect(Collectors.toSet());
            lraMethods.forEach((m, a) -> {
                if (a.stream().map(AnnotationInstance::name).anyMatch(InspectionService.COMPENSATE::equals)) {
                    if (!returnTypes.contains(m.returnType().name())) {
                        throw new DeploymentException("Invalid return type " + m.returnType() + " of compensating method " + m.name());
                    }
                }
                if (a.stream().map(AnnotationInstance::name).anyMatch(InspectionService.AFTER_LRA::equals)) {
                    if (!returnTypes.contains(m.returnType().name())) {
                        throw new DeploymentException("Invalid return type " + m.returnType() + " of after method " + m.name());
                    }
                }
            });

        }
    }

    public Map<Class<?>, Bean<?>> lraCdiBeanReferences() {
        return lraCdiBeanReferences;
    }

    void runtimeIndex(DotName fqdn) {
        if (fqdn == null) return;
        LOGGER.fine("Indexing " + fqdn);
        ClassInfo classInfo = null;
        try {
            classInfo = indexer.index(classLoader.getResourceAsStream(fqdn.toString().replace('.', '/') + ".class"));
            // look also for extended classes
            runtimeIndex(classInfo.superName());
            // and implemented interfaces
            classInfo.interfaceNames().forEach(this::runtimeIndex);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Unable to index referenced class.", e);
        }
    }

    private IndexView existingIndexFileReader(List<URL> indexUrls) throws IOException {
        List<IndexView> indices = new ArrayList<>();
        for (URL indexURL : indexUrls) {
            try (InputStream indexIS = indexURL.openStream()) {
                LOGGER.log(Level.INFO, "Adding Jandex index at {0}", indexURL.toString());
                indices.add(new IndexReader(indexIS).read());
            } catch (IOException ex) {
                throw new IOException("Attempted to read from previously-located index file "
                        + indexURL + " but the index cannot be found", ex);
            }
        }
        return indices.size() == 1 ? indices.get(0) : CompositeIndex.create(indices);
    }

    private List<URL> findIndexFiles(String... indexPaths) throws IOException {
        List<URL> result = new ArrayList<>();
        for (String indexPath : indexPaths) {
            Enumeration<URL> urls = classLoader.getResources(indexPath);
            while (urls.hasMoreElements()) {
                result.add(urls.nextElement());
            }
        }
        return result;
    }

    public IndexView getIndex() {
        return index;
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