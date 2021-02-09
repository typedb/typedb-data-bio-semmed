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
import grakn.client.GraknClient;
import grakn.common.collection.Either;
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
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static grakn.client.GraknClient.Session.Type.DATA;
import static grakn.client.GraknClient.Session.Type.SCHEMA;
import static grakn.client.GraknClient.Transaction.Type.WRITE;
import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Migrator {

    public static final int BATCH_SIZE = 64;
    public static final int PARALLELISATION_MAX = Runtime.getRuntime().availableProcessors();
    public static final String DATABASE_NAME = "biograkn-semmed";

    private static final Logger LOG = LoggerFactory.getLogger(Migrator.class);
    private static final String SCHEMA_GQL = "schema/biograkn-semmed.gql";
    private static final String SRC_CONCEPTS_CSV = "GENERIC_CONCEPT.csv";
    private static final String SRC_CITATIONS_CSV = "CITATIONS.csv";
    private static final String SRC_SENTENCES_CSV = "SENTENCE.csv";
    private static final String SRC_PREDICATIONS_CSV = "PREDICATION.csv";
    private static final String SRC_PREDICATIONS_AUX_CSV = "PREDICATION_AUX.csv";
    private static final String SRC_ENTITIES_CSV = "ENTITY.csv";
    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#,###.00");
    private static int exitStatus = 0;

    private static final Map<String, BiConsumer<GraknClient.Transaction, String[]>> SRC_CSV_WRITERS = map(
            pair(SRC_CONCEPTS_CSV, ConceptsWriter::write),
            pair(SRC_CITATIONS_CSV, CitationsWriter::write)
//            pair(SRC_SENTENCES_CSV, SentencesWriter::write),
//            pair(SRC_PREDICATIONS_CSV, PredicationsWriter::write),
//            pair(SRC_PREDICATIONS_AUX_CSV, PredicationsWriter::write),
//            pair(SRC_ENTITIES_CSV, EntitiesWriter::write)
    );

    private final AtomicBoolean hasError;
    private final ExecutorService executor;
    private final GraknClient client;
    private final Path source;
    private final int parallelisation;
    private final int batch;

    public Migrator(GraknClient client, Path source, int parallelisation, int batch) {
        assert parallelisation < Runtime.getRuntime().availableProcessors();
        this.client = client;
        this.source = source;
        this.parallelisation = parallelisation;
        this.batch = batch;
        executor = Executors.newFixedThreadPool(parallelisation, new NamedThreadFactory(DATABASE_NAME));
        hasError = new AtomicBoolean(false);
    }

    private static class Done {
        private static final Done INSTANCE = new Done();
    }

    public static void debug(String message, Object... objects) {
        if (LOG.isDebugEnabled()) LOG.debug(message, objects);
    }

    public void info(String message, Object... objects) {
        LOG.info(message, objects);
    }

    private void validate() {
        SRC_CSV_WRITERS.keySet().forEach(file -> {
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

    private void run() throws FileNotFoundException, InterruptedException {
        try (GraknClient.Session session = client.session(DATABASE_NAME, DATA)) {
            for (Map.Entry<String, BiConsumer<GraknClient.Transaction, String[]>> sourceCSVWriter : SRC_CSV_WRITERS.entrySet()) {
                if (!hasError.get()) asyncMigrate(session, sourceCSVWriter.getKey(), sourceCSVWriter.getValue());
            }
        }
    }

    private void asyncMigrate(GraknClient.Session session, String filename,
                              BiConsumer<GraknClient.Transaction, String[]> writerFn)
            throws FileNotFoundException, InterruptedException {
        info("async-migrate (start): {}", filename);
        LinkedBlockingQueue<Either<List<String[]>, Done>> queue = new LinkedBlockingQueue<>(parallelisation * 4);
        List<CompletableFuture<Void>> asyncWrites = new ArrayList<>(parallelisation);
        for (int i = 0; i < parallelisation; i++) {
            asyncWrites.add(asyncWrite(i + 1, filename, session, queue, writerFn));
        }
        bufferedRead(filename, queue);
        CompletableFuture.allOf(asyncWrites.toArray(new CompletableFuture[0])).join();
        info("async-migrate (end): {}", filename);
    }

    private CompletableFuture<Void> asyncWrite(int id, String filename, GraknClient.Session session,
                                               LinkedBlockingQueue<Either<List<String[]>, Done>> queue,
                                               BiConsumer<GraknClient.Transaction, String[]> writerFn) {
        return CompletableFuture.runAsync(() -> {
            debug("async-writer-{} (start): {}", id, filename);
            Either<List<String[]>, Done> queueItem;
            try {
                while ((queueItem = queue.take()).isFirst() && !hasError.get()) {
                    try (GraknClient.Transaction tx = session.transaction(WRITE)) {
                        List<String[]> csvLines = queueItem.first();
                        csvLines.forEach(csv -> {
                            debug("async-writer-{}: {}", id, Arrays.toString(csv));
                            writerFn.accept(tx, csv);
                        });
                        tx.commit();
                    }
                }
                assert queueItem.isSecond() || hasError.get();
                if (queueItem.isSecond()) queue.put(queueItem);
            } catch (Throwable e) {
                hasError.set(true);
                LOG.error("async-writer-" + id + ": " + e.getMessage());
                throw new RuntimeException(e);
            } finally {
                debug("async-writer-{} (end): {}", id, filename);
            }
        }, executor);
    }

    private void bufferedRead(String filename, LinkedBlockingQueue<Either<List<String[]>, Done>> queue)
            throws InterruptedException, FileNotFoundException {
        Iterator<String> iterator = newBufferedReader(filename).lines().iterator();
        ArrayList<String[]> csvLines = new ArrayList<>(batch);
        int count = 0;
        Instant startRead = Instant.now();
        Instant startBatch = Instant.now();
        while (iterator.hasNext() && !hasError.get()) {
            count++;
            String[] csv = parseCSV(iterator.next());
            debug("buffered-read (line {}): {}", count, Arrays.toString(csv));
            csvLines.add(csv);
            if (csvLines.size() == batch || !iterator.hasNext()) {
                queue.put(Either.first(csvLines));
                csvLines = new ArrayList<>(batch);
            }
            if (count % 10_000 == 0) {
                Instant endBatch = Instant.now();
                double rate = calculateRate(10_000, startBatch, endBatch);
                info("buffered-read {source: {}, progress: {}, rate: {}/s}",
                     filename, countFormat.format(count), decimalFormat.format(rate));
                startBatch = Instant.now();
            }
        }
        queue.put(Either.second(Done.INSTANCE));
        Instant endRead = Instant.now();
        double rate = calculateRate(count, startRead, endRead);
        info("buffered-read {total: {}, rate: {}/s}", countFormat.format(count), decimalFormat.format(rate));
    }

    private double calculateRate(double count, Instant start, Instant end) {
        return count * Duration.ofSeconds(1).toMillis() / Duration.between(start, end).toMillis();
    }

    private BufferedReader newBufferedReader(String csvName) throws FileNotFoundException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(source.resolve(csvName).toFile())));
    }

    private String[] parseCSV(String line) {
        String[] l = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        for (int i = 0; i < l.length; i++) {
            if (l[i].startsWith("\"") && l[i].endsWith("\"")) l[i] = l[i].substring(1, l[i].length() - 1);
            if (l[i].equals("\\N") || l[i].toLowerCase().equals("null")) l[i] = null;
        }
        return l;
    }

    private static String printDuration(Instant start, Instant end) {
        return Duration.between(start, end).toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }

    public static void main(String[] args) {
        Instant start = Instant.now();
        try {
            Options options = Options.parseCommandLine(args);
            if (options == null) System.exit(0);
            if (!options.source().toFile().isDirectory()) {
                throw new RuntimeException("Invalid data directory: " + options.source().toString());
            } else if (options.parallelisation() <= 0 || options.parallelisation() > PARALLELISATION_MAX) {
                throw new RuntimeException("Invalid parallelisation config: has to be greater than 0 and at most the number of CPU cores");
            } else if (options.batch() <= 0) {
                throw new RuntimeException("Invalid batch size: has to be larger than 0");
            } else {
                LOG.info("Source directory : {}", options.source().toString());
                LOG.info("Grakn address    : {}", options.grakn());
                LOG.info("Database name    : {}", options.database());
                LOG.info("Parallelisation  : {}", options.parallelisation());
                LOG.info("Batch size       : {}", options.batch());
            }

            Migrator migrator = null;
            try (GraknClient client = GraknClient.core(options.grakn())) {
                Runtime.getRuntime().addShutdownHook(
                        NamedThreadFactory.create(Migrator.class, "shutdown").newThread(client::close)
                );
                migrator = new Migrator(client, options.source(), options.parallelisation(), options.batch());
                migrator.validate();
                migrator.initialise();
                migrator.run();
            } finally {
                if (migrator != null) migrator.executor.shutdown();
            }
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            LOG.error("TERMINATED WITH ERROR");
            exitStatus = 1;
        } finally {
            Instant end = Instant.now();
            LOG.info("BioGrakn SemMed Migrator completed in: {}", printDuration(start, end));
            System.exit(exitStatus);
        }
    }
}
