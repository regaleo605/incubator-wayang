/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.optimizer.costs;

import org.apache.wayang.core.api.Configuration;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Estimates the {@link LoadProfile} of some executable artifact that takes some input data quanta and produces them.
 */
public interface LoadProfileEstimator {

    /**
     * Estimates a {@link LoadProfile}.
     *
     * @param context provides parameters for the estimation
     * @return the {@link LoadProfile}
     */
    LoadProfile estimate(EstimationContext context);

    /**
     * Nest a sub-instance.
     *
     * @param loadProfileEstimator the sub-instance
     */
    void nest(LoadProfileEstimator loadProfileEstimator);

    /**
     * Retrieve nested instances.
     *
     * @return the nested instances
     */
    Collection<LoadProfileEstimator> getNestedEstimators();

    /**
     * Retrieve the {@link Configuration} key for this instance.
     *
     * @return the key or {@code null} if none
     */
    String getConfigurationKey();

    /**
     * Retrieve the {@link Configuration} keys for this and all nested instances.
     * @return the keys (no {@code nulls})
     */
    default Collection<String> getConfigurationKeys() {
        Collection<String> keys = new LinkedList<>();
        final String key = this.getConfigurationKey();
        if (key != null) {
            keys.add(key);
        }
        for (LoadProfileEstimator nestedEstimator : this.getNestedEstimators()) {
            keys.addAll(nestedEstimator.getConfigurationKeys());
        }
        return keys;
    }

    /**
     * Retrieve the {@link Configuration} template key if any. Usually, this is the {@link Configuration} key
     * suffixed by {@code .template}.
     *
     * @return the template key or {@code null} if none
     */
    default String getTemplateKey() {
        final String configKey = this.getConfigurationKey();
        return configKey == null ? null : configKey + ".template";
    }

    /**
     * Retrieve the {@link Configuration} template keys for this and all nested instances.
     * @return the template keys (no {@code nulls})
     */
    default Collection<String> getTemplateKeys() {
        Collection<String> templateKeys = new LinkedList<>();
        final String templateKey = this.getTemplateKey();
        if (templateKey != null) {
            templateKeys.add(templateKey);
        }
        for (LoadProfileEstimator nestedEstimator : this.getNestedEstimators()) {
            templateKeys.addAll(nestedEstimator.getTemplateKeys());
        }
        return templateKeys;
    }

    /**
     * <i>Optional operation.</i> Copy this instance.
     *
     * @return the copy
     */
    default LoadProfileEstimator copy() {
        throw new UnsupportedOperationException();
    }

}
