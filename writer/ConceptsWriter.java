/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.vaticle.typedb.data.bio.semmed.writer;

import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.vaticle.typedb.data.bio.semmed.Migrator.debug;
import static com.vaticle.typeql.lang.TypeQL.insert;
import static com.vaticle.typeql.lang.TypeQL.var;

public class ConceptsWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ConceptsWriter.class);

    public static void write(TypeDBTransaction tx, String[] csv) {
        assert csv.length == 3;
        if (csv[0] == null) throw new RuntimeException("Null Concept ID in csv: " + Arrays.toString(csv));
        else if (csv[1] == null) {
            LOG.error("Null Concept CUI in csv: " + Arrays.toString(csv));
            return;
        }

        int id = Integer.parseInt(csv[0]);
        String cui = csv[1];
        String name = csv[2];

        TypeQLInsert query = insert(
                var().isa("concept").has("id", id).has("cui", cui).has("name", name)
        );
        debug("concept-writer: {}", query);
        tx.query().insert(query);
    }
}
