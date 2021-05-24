/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.opensearch.jdbc.config;

public class IntConnectionProperty extends ConnectionProperty<Integer> {

    public IntConnectionProperty(String key) {
        super(key);
    }

    @Override
    protected Integer parseValue(Object value) throws ConnectionPropertyException {

        if (value == null) {
            return getDefault();
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException nfe) {
                // invalid value
            }
        }

        throw new ConnectionPropertyException(getKey(),
                String.format("Property %s requires a valid integer. Invalid property value %s. ", getKey(), value));
    }

    @Override
    public Integer getDefault() {
        return 0;
    }
}