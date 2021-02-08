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

import grakn.client.GraknClient;
import graql.lang.query.GraqlInsert;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.Map;

import static biograkn.semmed.Migrator.debug;
import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static graql.lang.Graql.insert;
import static graql.lang.Graql.var;
import static java.lang.Integer.parseInt;

public class CitationsWriter {

    private static final Map<String, Month> months = map(
            pair("jan", Month.JANUARY),
            pair("feb", Month.FEBRUARY),
            pair("mar", Month.MARCH),
            pair("apr", Month.APRIL),
            pair("may", Month.MAY),
            pair("jun", Month.JUNE),
            pair("jul", Month.JULY),
            pair("aug", Month.AUGUST),
            pair("sep", Month.SEPTEMBER),
            pair("oct", Month.OCTOBER),
            pair("nov", Month.NOVEMBER),
            pair("dec", Month.DECEMBER),
            pair("winter", Month.DECEMBER),
            pair("spring", Month.MARCH),
            pair("summer", Month.JUNE),
            pair("autumn", Month.SEPTEMBER),
            pair("1", Month.JANUARY),
            pair("01", Month.JANUARY),
            pair("2", Month.FEBRUARY),
            pair("02", Month.FEBRUARY),
            pair("3", Month.MARCH),
            pair("03", Month.MARCH),
            pair("4", Month.APRIL),
            pair("04", Month.APRIL),
            pair("5", Month.MAY),
            pair("05", Month.MAY),
            pair("6", Month.JUNE),
            pair("06", Month.JUNE),
            pair("7", Month.JULY),
            pair("07", Month.JULY),
            pair("8", Month.AUGUST),
            pair("08", Month.AUGUST),
            pair("9", Month.SEPTEMBER),
            pair("09", Month.SEPTEMBER),
            pair("10", Month.OCTOBER),
            pair("11", Month.NOVEMBER),
            pair("12", Month.DECEMBER)
    );

    public static void write(GraknClient.Transaction tx, String[] csv) {
        assert csv.length == 5;
        if (csv[0] == null) throw new RuntimeException("Null Citation PMID in csv: " + Arrays.toString(csv));

        int pmid = parseInt(csv[0]);
        String issn = csv[1];
        // LocalDateTime dp = parseDP(csv[2]);
        LocalDateTime edat = parseEdat(csv[3]);
        int pyear = parseInt(csv[4]);

        GraqlInsert query = insert(
                var().isa("citation").has("pmid", pmid).has("issn", issn)
                        .has("edat", edat).has("pyear", pyear)
        );
        debug("citation-writer: {}", query);
        tx.query().insert(query);
    }

    private static LocalDateTime parseDP(String dp) {
        String[] tokens = dp.split(" ");
        assert tokens.length <= 3;

        int year = parseInt(tokens[0].split("-")[0]);
        Month month = Month.JANUARY;
        int day = 1;
        if (tokens.length > 1) month = months.get(tokens[1].split("-")[0].toLowerCase());
        if (tokens.length > 2) day = parseInt(tokens[2].split("-")[0]);

        return LocalDateTime.of(year, month, day, 0, 0, 0);
    }

    private static LocalDateTime parseEdat(String edat) {
        String[] tokens = edat.split("-");
        return LocalDateTime.of(parseInt(tokens[0]), Month.of(parseInt(tokens[1])), parseInt(tokens[2]), 0, 0, 0);
    }
}
