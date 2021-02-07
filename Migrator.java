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

import biograkn.semmed.loader.CitationsLoader;
import biograkn.semmed.loader.ConceptsLoader;
import biograkn.semmed.loader.EntitiesLoader;
import biograkn.semmed.loader.PredicationsLoader;
import biograkn.semmed.loader.SentencesLoader;
import grakn.client.GraknClient;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import static grakn.client.GraknClient.Session.Type.DATA;
import static grakn.client.GraknClient.Session.Type.SCHEMA;
import static grakn.client.GraknClient.Transaction.Type.WRITE;
import static grakn.common.collection.Collections.list;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(Migrator.class);
    private static final String DATABASE_NAME = "biograkn-semmed";
    private static final String SCHEMA_GQL = "schema/biograkn-semmed.gql";
    private static final String SRC_CONCEPTS_CSV = "GENERIC_CONCEPT.csv";
    private static final String SRC_CITATIONS_CSV = "CITATIONS.csv";
    private static final String SRC_SENTENCES_CSV = "SENTENCES.csv";
    private static final String SRC_PREDICATIONS_CSV = "PREDICATION.csv";
    private static final String SRC_ENTITIES_CSV = "ENTITY.csv";


    private final GraknClient client;
    private final Path source;

    public Migrator(GraknClient client, Path source) {
        this.client = client;
        this.source = source;
    }

    private void validate() {
        list(SRC_CONCEPTS_CSV, SRC_CITATIONS_CSV, SRC_PREDICATIONS_CSV).forEach(sourceFile -> {
            if (!source.resolve(sourceFile).toFile().isFile()) {
                throw new RuntimeException("Expected " + sourceFile + " file is missing.");
            }
        });
        if (client.databases().contains(DATABASE_NAME)) {
            throw new RuntimeException("There already exists a database with the name '" + DATABASE_NAME + "'");
        }
    }

    private void initialise() throws IOException {
        client.databases().create(DATABASE_NAME);
        try (GraknClient.Session session = client.session(DATABASE_NAME, SCHEMA)) {
            try (GraknClient.Transaction transaction = session.transaction(WRITE)) {
                GraqlDefine schema = Graql.parseQuery(new String(Files.readAllBytes(Paths.get(SCHEMA_GQL)), UTF_8));
                transaction.query().define(schema);
                transaction.commit();
            }
        }
    }

    private void run() {
        try (GraknClient.Session session = client.session(DATABASE_NAME, DATA)) {
            ConceptsLoader.load(session, source.resolve(SRC_CONCEPTS_CSV).toFile());
            CitationsLoader.load(session, source.resolve(SRC_CITATIONS_CSV).toFile());
            SentencesLoader.load(session, source.resolve(SRC_SENTENCES_CSV).toFile());
            PredicationsLoader.load(session, source.resolve(SRC_PREDICATIONS_CSV).toFile());
            EntitiesLoader.load(session, source.resolve(SRC_ENTITIES_CSV).toFile());
        }
    }

    private static String printDuration(Instant start, Instant end) {
        return Duration.between(start, end).toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }

    public static void main(String[] args) {
        try {
            if (args.length != 2) throw new RuntimeException("Two arguments are required: {source_path} {grakn_address}");
            Path source = Paths.get(args[0]);
            String address = args[1];
            if (!source.toFile().isDirectory()) {
                throw new RuntimeException("Invalid data directory: " + source.toString());
            } else {
                LOG.info("Source directory : {}", source.toString());
                LOG.info("Grakn address  : {}", address);
            }

            Instant start = Instant.now();
            try (GraknClient client = GraknClient.core(address)) {
                Migrator migrator = new Migrator(client, source);
                migrator.validate();
                migrator.initialise();
                migrator.run();
            }
            Instant end = Instant.now();

            LOG.info("BioGrakn SemMed Migrator completed in: {}", printDuration(start, end));
        } catch (Throwable e) {
            LOG.error(e.getMessage());
            LOG.error("BioGrakn SemMed Migrator terminated with error");
            System.exit(1);
        }
    }
}
