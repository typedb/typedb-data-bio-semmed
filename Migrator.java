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

package biograkn.semmed;

import grakn.client.GraknClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

public class Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(Migrator.class);

    private final GraknClient address;
    private final File data;

    public Migrator(GraknClient client, File data) {
        this.address = client;
        this.data = data;
    }

    private void run() {
        // TODO
    }

    private static String printDuration(Instant start, Instant end) {
        return Duration.between(start, end).toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }

    public static void main(String[] args) {
        try {
            if (args.length != 2) throw new RuntimeException("Two arguments are required: {data_path} {grakn_address}");
            File data = Paths.get(args[0]).toFile();
            String address = args[1];
            if (!data.isDirectory()) {
                throw new RuntimeException("Invalid data directory: " + data.toString());
            } else {
                LOG.info("Data directory : {}", data.toString());
                LOG.info("Grakn address  : {}", address);
            }

            Instant start = Instant.now();
            GraknClient client = GraknClient.core(address);
            Migrator migrator = new Migrator(client, data);
            migrator.run();
            Instant end = Instant.now();

            LOG.info("BioGrakn SemMed Migrator completed in: {}", printDuration(start, end));
        } catch (Throwable e) {
            LOG.error(e.getMessage());
            LOG.error("BioGrakn SemMed Migrator terminated with error");
            System.exit(1);
        }
    }
}
