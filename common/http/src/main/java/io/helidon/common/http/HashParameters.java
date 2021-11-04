/*
 * Copyright (c) 2017, 2021 Oracle and/or its affiliates.
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

package io.helidon.common.http;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * A {@link ConcurrentSkipListMap} based {@link Parameters} implementation with
 * case-insensitive keys and immutable {@link List} of values that needs to be copied on each write.
 */
public class HashParameters implements Parameters {

    private static final CharSequence[] EMPTY_STRING_LIST = new CharSequence[0];

    private final ConcurrentMap<CharSequence, CharSequence[]> content;

    public Map<CharSequence, CharSequence[]> content(){
        return content;
    }

    /**
     * Creates a new instance.
     */
    protected HashParameters() {
        this((Parameters) null);
    }

    /**
     * Creates a new instance from provided data.
     * Initial data are copied.
     *
     * @param initialContent initial content.
     */
    protected HashParameters(Map<String, List<String>> initialContent) {
        if (initialContent == null) {
            content = new ConcurrentHashMap<>();
        } else {
            content = new ConcurrentHashMap<>();
            for (Map.Entry<String, List<String>> entry : initialContent.entrySet()) {
                content.put(entry.getKey(), entry.getValue().toArray(new String[0]));
            }
        }
    }

    /**
     * Creates a new instance from provided data.
     * Initial data is copied.
     *
     * @param initialContent initial content.
     */
    protected HashParameters(Parameters initialContent) {
        this(initialContent == null ? null : initialContent.toMap());
    }

    /**
     * Creates a new empty instance {@link HashParameters}.
     *
     * @return a new instance of {@link HashParameters}.
     */
    public static HashParameters create() {
        return new HashParameters();
    }

    /**
     * Creates a new instance {@link HashParameters} from provided data. Initial data is copied.
     *
     * @param initialContent initial content.
     * @return a new instance of {@link HashParameters} initialized with the given content.
     */
    public static HashParameters create(Map<String, List<String>> initialContent) {
        return new HashParameters(initialContent);
    }

    /**
     * Creates a new instance {@link HashParameters} from provided data. Initial data is copied.
     *
     * @param initialContent initial content.
     * @return a new instance of {@link HashParameters} initialized with the given content.
     */
    public static HashParameters create(Parameters initialContent) {
        return new HashParameters(initialContent);
    }

    /**
     * Creates new instance of {@link HashParameters} as a concatenation of provided parameters.
     * Values for keys found across the provided parameters are "concatenated" into a {@link List} entry for their respective key
     * in the created {@link HashParameters} instance.
     *
     * @param parameters parameters to concatenate.
     * @return a new instance of {@link HashParameters} that represents the concatenation of the provided parameters.
     */
    public static HashParameters concat(Parameters... parameters) {
        if (parameters == null || parameters.length == 0) {
            return new HashParameters();
        }
        List<Map<String, List<String>>> prms = new ArrayList<>(parameters.length);
        for (Parameters p : parameters) {
            if (p != null) {
                prms.add(p.toMap());
            }
        }
        return concat(prms);
    }

    /**
     * Creates new instance of {@link HashParameters} as a concatenation of provided parameters.
     * Values for keys found across the provided parameters are "concatenated" into a {@link List} entry for their respective key
     * in the created {@link HashParameters} instance.
     *
     * @param parameters parameters to concatenate.
     * @return a new instance of {@link HashParameters} that represents the concatenation of the provided parameters.
     */
    public static HashParameters concat(Iterable<Parameters> parameters) {
        ArrayList<Map<String, List<String>>> prms = new ArrayList<>();
        for (Parameters p : parameters) {
            if (p != null) {
                prms.add(p.toMap());
            }
        }
        return concat(prms);
    }

    private static HashParameters concat(List<Map<String, List<String>>> prms) {
        if (prms.isEmpty()) {
            return new HashParameters();
        }
        if (prms.size() == 1) {
            return new HashParameters(prms.get(0));
        }

        Map<String, List<String>> composer = new HashMap<>();
        for (Map<String, List<String>> prm : prms) {
            for (Map.Entry<String, List<String>> entry : prm.entrySet()) {
                List<String> strings = composer.computeIfAbsent(entry.getKey(), k -> new ArrayList<>(entry.getValue().size()));
                strings.addAll(entry.getValue());
            }
        }
        return new HashParameters(composer);
    }

    private static List<String> internalListCopy(Iterable<String> values) {
        if (values == null) {
            return null;
        } else {
            List<String> result;
            if (values instanceof Collection) {
                result = new ArrayList<>((Collection<String>) values);
            } else {
                result = new ArrayList<>();
                for (String value : values) {
                    result.add(value);
                }
            }
            if (result.isEmpty()) {
                return null;
            } else {
                return Collections.unmodifiableList(result);
            }
        }
    }

    private static List<String> toStringList(CharSequence... sequences) {
        String[] resultArr = new String[sequences.length];
        for (int i = 0; i < sequences.length; i++) {
            resultArr[i] = sequences[i].toString();
        }
        return List.of(resultArr);
    }

    @Override
    public Optional<String> first(String name) {
        Objects.requireNonNull(name, "Parameter 'name' is null!");
        CharSequence[] values = content.getOrDefault(name, EMPTY_STRING_LIST);
        if (values.length > 0) {
            return Optional.of(values[0].toString());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<String> all(String name) {
        Objects.requireNonNull(name, "Parameter 'name' is null!");
        CharSequence[] values = content.getOrDefault(name, EMPTY_STRING_LIST);
        List<String> result = new ArrayList<>(values.length);
        for (CharSequence chs : values) {
            result.add(chs.toString());
        }
        return result;
    }

    @Override
    public List<String> put(String key, String... values) {
        CharSequence[] result;
        if (values == null || values.length == 0) {
            result = content.remove(key);
        } else {
            result = content.put(key, values.clone());
        }
        return result == null ? List.of() : toStringList(result);
    }

    @Override
    public List<String> put(String key, Iterable<String> values) {
        List<String> vs = internalListCopy(values);
        CharSequence[] result;
        if (vs == null) {
            result = content.remove(key);
        } else {
            result = content.put(key, vs.toArray(new CharSequence[0]));
        }
        return result == null ? List.of() : toStringList(result);
    }

    @Override
    public List<String> putIfAbsent(String key, String... values) {
        CharSequence[] result;
        if (values == null || values.length == 0) {
            result = content.get(key);
        } else {
            result = content.putIfAbsent(key, values.clone());
        }
        return result == null ? List.of() : toStringList(result);
    }

    @Override
    public List<String> putIfAbsent(String key, Iterable<String> values) {
        List<String> vls = internalListCopy(values);
        CharSequence[] result;
        if (vls == null) {
            result = content.get(key);
        } else {
            result = content.putIfAbsent(key, vls.toArray(new CharSequence[0]));
        }
        return result == null ? List.of() : toStringList(result);
    }

    @Override
    public List<String> computeIfAbsent(String key, Function<String, Iterable<String>> values) {
        CharSequence[] result = content.computeIfAbsent(key,
                k -> internalListCopy(values.apply(k.toString())).toArray(new CharSequence[0])
        );
        return toStringList(result);
    }

    @Override
    public List<String> computeSingleIfAbsent(String key, Function<String, String> value) {
        CharSequence[] result = content.computeIfAbsent(key, k -> {
            String v = value.apply(k.toString());
            if (v == null) {
                return null;
            } else {
                return new CharSequence[] {v};
            }
        });
        return result == null ? List.of() : toStringList(result);
    }

    @Override
    public HashParameters putAll(Parameters parameters) {
        if (parameters == null) {
            return this;
        }

        for (Map.Entry<String, List<String>> entry : parameters.toMap().entrySet()) {
            List<String> values = entry.getValue();
            if (values != null && !values.isEmpty()) {
                content.put(entry.getKey(), values.toArray(new CharSequence[0]));
            }
        }
        return this;
    }

    @Override
    public HashParameters add(String key, String... values) {
        return add((CharSequence) key, values);
    }

    @Override
    public HashParameters add(CharSequence key, CharSequence... values) {
        Objects.requireNonNull(key, "Parameter 'key' is null!");
        if (values == null || values.length == 0) {
            // do not necessarily create an entry in the map, simply immediately return
            return this;
        }

        content.compute(key, (s, arr) -> {
            if (arr == null) {
                return values;
            } else {
                CharSequence[] result = new CharSequence[arr.length + values.length];
                System.arraycopy(arr, 0, result, 0, arr.length);
                System.arraycopy(values, 0, result, arr.length, values.length);
                return result;
            }
        });
        return this;
    }

    @Override
    public HashParameters add(String key, Iterable<String> values) {
        return add((CharSequence) key, StreamSupport
                .stream(values.spliterator(), false)
                .map(CharSequence.class::cast)
                .toArray(CharSequence[]::new)
        );
    }

    @Override
    public HashParameters addAll(Parameters parameters) {
        if (parameters == null) {
            return this;
        }
        Map<String, List<String>> map = parameters.toMap();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            add(entry.getKey(), entry.getValue().toArray(new CharSequence[0]));
        }
        return this;
    }

    @Override
    public List<String> remove(String key) {
        CharSequence[] result = content.remove(key);
        return result == null ? Collections.emptyList() : toStringList(result);
    }

    @Override
    public Map<String, List<String>> toMap() {
        // deep copy
        Map<String, List<String>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<CharSequence, CharSequence[]> entry : content.entrySet()) {
            result.put(entry.getKey().toString(), toStringList(entry.getValue()));
        }
        return result;
    }

    @Override
    public String toString() {
        return content.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HashParameters)) {
            return false;
        }
        HashParameters that = (HashParameters) o;
        return content.equals(that.content);
    }

    @Override
    public int hashCode() {
        return content.hashCode();
    }
}
