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

package biograkn.semmed.writer;

import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLInsert;

import java.util.Arrays;

import static biograkn.semmed.Migrator.debug;
import static com.vaticle.typeql.lang.TypeQL.insert;
import static com.vaticle.typeql.lang.TypeQL.var;
import static java.lang.Integer.min;
import static java.lang.Integer.parseInt;

public class SentencesWriter {

    public static void write(TypeDBTransaction tx, String[] csv) {
        assert csv.length == 9;
        if (csv[0] == null) throw new RuntimeException("Null Sentence ID in CSV: " + Arrays.toString(csv));
        if (csv[1] == null) throw new RuntimeException("Null Citation PMID in CSV: " + Arrays.toString(csv));

        ThingVariable.Thing sentence = var().isa("sentence").has("id", parseInt(csv[0]));
        sentence = sentence.has("pmid", parseInt(csv[1]));
        if (csv[2] != null) sentence = sentence.has("type_", csv[2]);
        if (csv[3] != null) sentence = sentence.has("number", parseInt(csv[3]));
        if (csv[4] != null) sentence = sentence.has("start-index", parseInt(csv[4]));
        if (csv[5] != null) sentence = sentence.has("text", csv[5]);
        if (csv[6] != null) sentence = sentence.has("end-index", parseInt(csv[6]));
        if (csv[7] != null) sentence = sentence.has("section-header", csv[7]);
        if (csv[8] != null) sentence = sentence.has("normalized-section-header", csv[8]);

        TypeQLInsert query = insert(sentence);
        debug("sentences-writer: {}", query);
        tx.query().insert(query);
    }

    private static String truncate(String s, int t) {
        String str = s.substring(0, min(s.length(), t));
        if (str.endsWith("\\")) str = str.substring(0, str.length() - 1);
        return str;
    }
}
