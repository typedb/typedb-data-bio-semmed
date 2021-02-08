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

import biograkn.semmed.writer.CitationsWriter;
import biograkn.semmed.writer.ConceptsWriter;
import biograkn.semmed.writer.PredicationsWriter;
import grakn.client.GraknClient;
import grakn.common.concurrent.NamedThreadFactory;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static grakn.client.GraknClient.Session.Type.DATA;
import static grakn.client.GraknClient.Session.Type.SCHEMA;
import static grakn.client.GraknClient.Transaction.Type.WRITE;
import static grakn.common.collection.Collections.list;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Migrator {

    public static final int BATCH_SIZE = 32;
    public static final int PARALLELISATION_MAX = Runtime.getRuntime().availableProcessors() - 1;
    public static final String DATABASE_NAME = "biograkn-semmed";

    private static final Logger LOG = LoggerFactory.getLogger(Migrator.class);
    private static final String SCHEMA_GQL = "schema/biograkn-semmed.gql";
    private static final String SRC_CONCEPTS_CSV = "GENERIC_CONCEPT.csv";
    private static final String SRC_CITATIONS_CSV = "CITATIONS.csv";
    private static final String SRC_SENTENCES_CSV = "SENTENCES.csv";
    private static final String SRC_PREDICATIONS_CSV = "PREDICATION.csv";
    private static final String SRC_PREDICATIONS_AUX_CSV = "PREDICATION_AUX.csv";
    private static final String SRC_ENTITIES_CSV = "ENTITY.csv";


    private final ExecutorService executor;
    private final GraknClient client;
    private final Path source;
    private final int parallelisation;

    public Migrator(GraknClient client, Path source, int parallelisation) {
        assert parallelisation < Runtime.getRuntime().availableProcessors();
        this.client = client;
        this.source = source;
        this.parallelisation = parallelisation;
        executor = Executors.newFixedThreadPool(parallelisation + 1, new NamedThreadFactory(DATABASE_NAME));
    }

    private void validate() {
        // TODO: validate SRC_SENTENCES_CSV and SRC_ENTITIES_CSV exists
        list(SRC_CONCEPTS_CSV, SRC_CITATIONS_CSV, SRC_PREDICATIONS_CSV, SRC_PREDICATIONS_AUX_CSV).forEach(file -> {
            if (!source.resolve(file).toFile().isFile()) {
                throw new RuntimeException("Expected " + file + " file is missing.");
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

    private void run() throws FileNotFoundException {
        try (GraknClient.Session session = client.session(DATABASE_NAME, DATA)) {
            parallelisedWrite(session, SRC_CONCEPTS_CSV, ConceptsWriter::write);
            parallelisedWrite(session, SRC_CITATIONS_CSV, CitationsWriter::write);
            // parallelisedWrite(session, SRC_SENTENCES_CSV, SentencesWriter::write);
            parallelisedWrite(session, SRC_PREDICATIONS_CSV, PredicationsWriter::write);
            parallelisedWrite(session, SRC_PREDICATIONS_AUX_CSV, PredicationsWriter::write);
            // parallelisedWrite(session, SRC_ENTITIES_CSV, EntitiesWriter::write);
        }
    }

    private void parallelisedWrite(GraknClient.Session session, String csvName,
                                   BiConsumer<GraknClient.Transaction, String[]> writerFn) throws FileNotFoundException {
        BufferedReader reader = newReader(csvName);
        // TODO
    }

    private BufferedReader newReader(String csvName) throws FileNotFoundException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(source.resolve(csvName).toFile())));
    }

    private static String printDuration(Instant start, Instant end) {
        return Duration.between(start, end).toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }

    public static void main(String[] args) {
        try {
            Options options = Options.parseCommandLine(args);
            if (options == null) System.exit(0);
            if (!options.source().toFile().isDirectory()) {
                throw new RuntimeException("Invalid data directory: " + options.source().toString());
            } else if (!(options.parallelisation() > 0 && options.parallelisation() <= PARALLELISATION_MAX)) {
                throw new RuntimeException("Invalid parallelisation config: has to be greater than 0 and less than CPU cores");
            } else {
                LOG.info("Source directory : {}", options.source().toString());
                LOG.info("Grakn address    : {}", options.grakn());
                LOG.info("Database name    : {}", options.database());
                LOG.info("Parallelisation  : {}", options.parallelisation());
                LOG.info("Batch size       : {}", options.batch());
            }

            Instant start = Instant.now();
            try (GraknClient client = GraknClient.core(options.grakn())) {
                Migrator migrator = new Migrator(client, options.source(), options.parallelisation());
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
