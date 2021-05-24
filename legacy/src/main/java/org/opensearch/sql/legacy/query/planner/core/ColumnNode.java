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
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.query.planner.core;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.legacy.executor.format.Schema;
import org.opensearch.sql.legacy.expression.core.Expression;

/**
 * The definition of column node.
 */
@Builder
@Setter
@Getter
@ToString
public class ColumnNode {
    private String name;
    private String alias;
    private Schema.Type type;
    private Expression expr;

    public String columnName() {
        return Strings.isNullOrEmpty(alias) ? name : alias;
    }
}