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
import biograkn.semmed.writer.SentencesWriter;
import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.vaticle.typedb.client.api.connection.TypeDBSession.Type.DATA;
import static com.vaticle.typedb.client.api.connection.TypeDBSession.Type.SCHEMA;
import static com.vaticle.typedb.client.api.connection.TypeDBTransaction.Type.WRITE;
import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Migrator {

    public static final int DEFAULT_WRITE_BATCH_SIZE = 64;
    public static final int DEFAULT_READ_BATCH_SIZE = 512;
    public static final int DEFAULT_PARALLELISATION = Runtime.getRuntime().availableProcessors();
    public static final String DEFAULT_DATABASE_NAME = "biograkn-semmed";

    private static final Logger LOG = LoggerFactory.getLogger(Migrator.class);
    private static final String SCHEMA_GQL = "schema/biograkn-semmed.gql";
    private static final String SRC_CONCEPTS_CSV = "CONCEPTS.csv";
    private static final String SRC_CITATIONS_CSV = "CITATIONS.csv";
    private static final String SRC_SENTENCES_CSV = "SENTENCES.csv";
    private static final String SRC_PREDICATIONS_CSV = "PREDICATIONS.csv";
    private static final String SRC_PREDICATIONS_AUX_CSV = "PREDICATIONS_AUX.csv";
    private static final String SRC_ENTITIES_CSV = "ENTITIES.csv";
    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#,###.00");
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withEscape('\\').withIgnoreSurroundingSpaces().withNullString("");
    private static int exitStatus = 0;

    private static final List<Pair<String, BiConsumer<TypeDBTransaction, String[]>>> SRC_CSV_WRITERS = list(
//            pair(SRC_CONCEPTS_CSV, ConceptsWriter::write),
//            pair(SRC_CITATIONS_CSV, CitationsWriter::write),
            pair(SRC_SENTENCES_CSV, SentencesWriter::write)
//            pair(SRC_PREDICATIONS_CSV, PredicationsWriter::write),
//            pair(SRC_PREDICATIONS_AUX_CSV, PredicationsWriter::write),
//            pair(SRC_ENTITIES_CSV, EntitiesWriter::write)
    );

    private final AtomicBoolean hasError;
    private final ExecutorService executor;
    private final TypeDBClient client;
    private final String database;
    private final Path source;
    private final int parallelisation;
    private final int batch;
    private final int batchGroup;

    public Migrator(TypeDBClient client, String database, Path source, int parallelisation, int batch) {
        assert parallelisation < DEFAULT_PARALLELISATION;
        this.client = client;
        this.database = database;
        this.source = source;
        this.parallelisation = parallelisation;
        this.batch = batch;
        this.batchGroup = DEFAULT_READ_BATCH_SIZE / batch;
        executor = Executors.newFixedThreadPool(parallelisation, new NamedThreadFactory(DEFAULT_DATABASE_NAME));
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
        SRC_CSV_WRITERS.forEach(pair -> {
            if (!source.resolve(pair.first()).toFile().isFile()) {
                throw new RuntimeException("Expected " + pair + " file is missing.");
            }
        });
        if (client.databases().contains(database)) {
            client.databases().get(database).delete();
            // TODO: throw new RuntimeException("There already exists a database with the name '" + database + "'");
        }
    }

    private void initialise() throws IOException {
        client.databases().create(database);
        try (TypeDBSession session = client.session(database, SCHEMA)) {
            try (TypeDBTransaction transaction = session.transaction(WRITE)) {
                TypeQLDefine schema = TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get(SCHEMA_GQL)), UTF_8));
                transaction.query().define(schema);
                transaction.commit();
            }
        }
    }

    private void run() throws IOException, InterruptedException {
        try (TypeDBSession session = client.session(database, DATA)) {
            for (Pair<String, BiConsumer<TypeDBTransaction, String[]>> sourceCSVWriter : SRC_CSV_WRITERS) {
                if (!hasError.get()) asyncMigrate(session, sourceCSVWriter.first(), sourceCSVWriter.second());
            }
        }
    }

    private void asyncMigrate(TypeDBSession session, String filename, BiConsumer<TypeDBTransaction, String[]> writerFn)
            throws IOException, InterruptedException {
        info("async-migrate (start): {}", filename);
        LinkedBlockingQueue<Either<List<List<String[]>>, Done>> queue = new LinkedBlockingQueue<>(parallelisation * 4);
        List<CompletableFuture<Void>> asyncWrites = new ArrayList<>(parallelisation);
        for (int i = 0; i < parallelisation; i++) {
            asyncWrites.add(asyncWrite(i + 1, filename, session, queue, writerFn));
        }
        bufferedRead(filename, queue);
        CompletableFuture.allOf(asyncWrites.toArray(new CompletableFuture[0])).join();
        info("async-migrate (end): {}", filename);
    }

    private CompletableFuture<Void> asyncWrite(int id, String filename, TypeDBSession session,
                                               LinkedBlockingQueue<Either<List<List<String[]>>, Done>> queue,
                                               BiConsumer<TypeDBTransaction, String[]> writerFn) {
        return CompletableFuture.runAsync(() -> {
            debug("async-writer-{} (start): {}", id, filename);
            Either<List<List<String[]>>, Done> queueItem;
            try {
                while ((queueItem = queue.take()).isFirst() && !hasError.get()) {
                    List<List<String[]>> csvRecordsGroup = queueItem.first();
                    for (List<String[]> csvRecords : csvRecordsGroup) {
                        try (TypeDBTransaction tx = session.transaction(WRITE)) {
                            csvRecords.forEach(csv -> {
                                debug("async-writer-{}: {}", id, csv);
                                writerFn.accept(tx, csv);
                            });
                            tx.commit();
                        }
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

    private void bufferedRead(String filename, LinkedBlockingQueue<Either<List<List<String[]>>, Done>> queue)
            throws InterruptedException, IOException {
        Iterator<String> iterator = newBufferedReader(filename).lines().iterator();
        List<List<String[]>> csvLinesGroup = new ArrayList<>(batchGroup);
        List<String[]> csvLines = new ArrayList<>(batch);

        int count = 0;
        Instant startRead = Instant.now();
        Instant startBatch = Instant.now();
        while (iterator.hasNext() && !hasError.get()) {
            count++;
            String[] csv = parseCSV(iterator.next());
            debug("buffered-read (line {}): {}", count, Arrays.toString(csv));
            csvLines.add(csv);
            if (csvLines.size() == batch || !iterator.hasNext()) {
                csvLinesGroup.add(csvLines);
                csvLines = new ArrayList<>(batch);
                if (csvLinesGroup.size() == batchGroup || !iterator.hasNext()) {
                    queue.put(Either.first(csvLinesGroup));
                    csvLinesGroup = new ArrayList<>(batchGroup);
                }
            }

            if (count % 10_000 == 0) {
                Instant endBatch = Instant.now();
                double rate = calculateRate(10_000, startBatch, endBatch);
                double average = calculateRate(count, startRead, endBatch);
                info("buffered-read {source: {}, progress: {}, rate: {}/s, average: {}/s}",
                     filename, countFormat.format(count), decimalFormat.format(rate), decimalFormat.format(average));
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

    private String[] parseCSV(String line) throws IOException {
        CSVRecord csv = CSVParser.parse(line, CSV_FORMAT).getRecords().get(0);
        String[] arr = new String[csv.size()];
        for (int i = 0; i < csv.size(); i++) {
            arr[i] = csv.get(i);
            if (arr[i] != null) {
                if ((arr[i].equals("\\N") || arr[i].toLowerCase().equals("null"))) arr[i] = null;
                else arr[i] = escapeDoubleQuotes(arr[i]);
            }
        }
        return arr;
    }

    private String escapeDoubleQuotes(String toFormat) {
        return toFormat.replaceAll("\"", "\\\\\"");
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
            Optional<Options> parsedOptions = Options.parseCommandLine(args);
            if (!parsedOptions.isPresent()) System.exit(0);

            Options options = parsedOptions.get();

            if (!options.source().toFile().isDirectory()) {
                throw new RuntimeException("Invalid data directory: " + options.source().toString());
            } else if (options.parallelisation() <= 0) {
                throw new RuntimeException("Invalid parallelisation config: has to be greater than 0");
            } else if (options.batch() <= 0) {
                throw new RuntimeException("Invalid batch size: has to be greater than 0");
            } else {
                LOG.info("Source directory : {}", options.source().toString());
                LOG.info("TypeDB address   : {}", options.typedb());
                LOG.info("Database name    : {}", options.database());
                LOG.info("Parallelisation  : {}", options.parallelisation());
                LOG.info("Batch size       : {}", options.batch());
            }

            Migrator migrator = null;
            try (TypeDBClient client = TypeDB.coreClient(options.typedb(), DEFAULT_PARALLELISATION)) {
                Runtime.getRuntime().addShutdownHook(
                        NamedThreadFactory.create(Migrator.class, "shutdown").newThread(client::close)
                );
                migrator = new Migrator(client, options.database(), options.source(), options.parallelisation(), options.batch());
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
