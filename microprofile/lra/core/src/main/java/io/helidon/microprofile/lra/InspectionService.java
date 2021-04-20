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
 *
 */

package io.helidon.microprofile.lra;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;
import javax.inject.Inject;

import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

@ApplicationScoped
public class InspectionService {

    private final IndexView index;

    @Inject
    public InspectionService(LRACdiExtension lraCdiExtension) {
        this.index = lraCdiExtension.getIndex();
    }

    public Set<AnnotationInstance> lookUpLraAnnotations(Method method) {
        Map<String, AnnotationInstance> annotations = new HashMap<>();
        ClassInfo declaringClazz = index.getClassByName(DotName.createSimple(method.getDeclaringClass().getName()));
        if (declaringClazz == null) {
            throw new DeploymentException("Can't find indexed declaring class of method " + method);
        }
        String methodName = method.getName();
        int parameterCount = method.getParameterCount();
        String paramHash = Arrays.stream(method.getParameterTypes()).map(Class::getName).collect(Collectors.joining());
        MethodInfo methodInfo = declaringClazz.methods().stream()
                .filter(m -> m.name().equals(methodName))
                .filter(m -> m.parameters().size() == parameterCount)
                .filter(m -> m.parameters().stream().map(p -> p.name().toString()).collect(Collectors.joining()).equals(paramHash))
                .findFirst()
                .orElseThrow(() -> new DeploymentException("LRA method " + method
                        + " not found indexed in class " + declaringClazz.name()));
        deepScanLraMethod(declaringClazz, annotations, methodInfo.name(), methodInfo.parameters().toArray(new Type[0]));
        HashSet<AnnotationInstance> result = new HashSet<>(annotations.values());

        if (annotations.containsKey(DotName.createSimple(Compensate.class.getName()).toString())) {
            // compensate can't be accompanied by class level LRA
            return result;
        }

        AnnotationInstance classLevelLraAnnotation = deepScanClassLevelLraAnnotation(declaringClazz);
        if (classLevelLraAnnotation != null) {
            // add class level @LRA only if not declared by method
            result.add(classLevelLraAnnotation);
        }
        return result;
    }

    public IndexView index() {
        return index;
    }

    AnnotationInstance deepScanClassLevelLraAnnotation(ClassInfo classInfo) {
        if (classInfo == null) return null;
        AnnotationInstance lraClassAnnotation = classInfo.classAnnotation(DotName.createSimple(LRA.class.getName()));
        if (lraClassAnnotation != null) {
            return lraClassAnnotation;
        }
        // extends
        lraClassAnnotation = deepScanClassLevelLraAnnotation(index.getClassByName(classInfo.superName()));
        if (lraClassAnnotation != null) {
            return lraClassAnnotation;
        }
        // implements
        for (DotName interfaceName : classInfo.interfaceNames()) {
            lraClassAnnotation = deepScanClassLevelLraAnnotation(index.getClassByName(interfaceName));
            if (lraClassAnnotation != null) {
                return lraClassAnnotation;
            }
        }
        return null;
    }

    void deepScanLraMethod(ClassInfo classInfo, Map<String, AnnotationInstance> annotations, String methodName, Type... parameters) {
        if (classInfo == null) return;
        // add only those not already present(overriding)
        MethodInfo method = classInfo.method(methodName, parameters);
        if (method == null) return;
        method.asMethod()
                .annotations()
                .forEach(a -> annotations.putIfAbsent(a.name().toString(), a));
        // extends
        deepScanLraMethod(index.getClassByName(classInfo.superName()), annotations, methodName, parameters);
        // implements
        for (DotName interfaceName : classInfo.interfaceNames()) {
            deepScanLraMethod(index.getClassByName(interfaceName), annotations, methodName, parameters);
        }
    }
}
