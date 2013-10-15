/*
 *   This software is licensed under the Apache 2 license, quoted below.
 *
 *   Copyright 2012-2013 Martin Bednar
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *   use this file except in compliance with the License. You may obtain a copy of
 *   the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations under
 *   the License.
 */

package org.elasticsearch.river.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class CSVRiver extends AbstractRiverComponent implements River {

    private final ThreadPool threadPool;
    private final Client client;
    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private String folderName;
    private String filenamePattern;
    private volatile BulkRequestBuilder currentRequest;
    private volatile boolean closed = false;
    private List<Object> csvFields;
    private TimeValue poll;
    private Thread thread;
    private char escapeCharacter;
    private char quoteCharacter;
    private char separator;
    private AtomicInteger onGoingBulks = new AtomicInteger();
    private int bulkThreshold;

    @SuppressWarnings({"unchecked"})
    @Inject
    public CSVRiver(RiverName riverName, RiverSettings settings, Client client, ThreadPool threadPool) {
        super(riverName, settings);
        this.client = client;
        this.threadPool = threadPool;

        if (settings.settings().containsKey("csv_file")) {
            Map<String, Object> csvSettings = (Map<String, Object>) settings.settings().get("csv_file");
            folderName = XContentMapValues.nodeStringValue(csvSettings.get("folder"), null);
            filenamePattern = XContentMapValues.nodeStringValue(csvSettings.get("filename_pattern"), ".*\\.csv$");
            csvFields = XContentMapValues.extractRawValues("fields", csvSettings);
            poll = XContentMapValues.nodeTimeValue(csvSettings.get("poll"), TimeValue.timeValueMinutes(60));
            escapeCharacter = XContentMapValues.nodeStringValue(csvSettings.get("escape_character"), String.valueOf(CSVReader.DEFAULT_ESCAPE_CHARACTER)).charAt(0);
            separator = XContentMapValues.nodeStringValue(csvSettings.get("field_separator"), String.valueOf(CSVReader.DEFAULT_SEPARATOR)).charAt(0);
            quoteCharacter = XContentMapValues.nodeStringValue(csvSettings.get("quote_character"), String.valueOf(CSVReader.DEFAULT_QUOTE_CHARACTER)).charAt(0);
        }

        logger.info("creating csv stream river for [{}] with pattern [{}]", folderName, filenamePattern);

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "csv_type");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            bulkThreshold = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_threshold"), 10);
        } else {
            indexName = riverName.name();
            typeName = "csv_type";
            bulkSize = 100;
            bulkThreshold = 10;
        }

    }

    @Override
    public void start() {
        logger.info("starting csv stream");
        currentRequest = client.prepareBulk();
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "CSV processor").newThread(new CSVConnector());
        thread.start();
    }

    @Override
    public void close() {
        logger.info("closing csv stream river");
        this.closed = true;
        thread.interrupt();
    }

    private void delay() {
        if (poll.millis() > 0L) {
            logger.info("next run waiting for {}", poll);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException e) {
                logger.error("Error during waiting.", e, (Object) null);
            }
        }
    }


    private void processBulkIfNeeded(Boolean force) {
        if (force || currentRequest.numberOfActions() >= bulkSize) {
            logger.warn("sending {} actions", currentRequest.numberOfActions());
            // execute the bulk operation
            int currentOnGoingBulks = onGoingBulks.incrementAndGet();
            if (currentOnGoingBulks > bulkThreshold) {
                onGoingBulks.decrementAndGet();
                logger.warn("ongoing bulk, [{}] crossed threshold [{}], waiting", onGoingBulks, bulkThreshold);
                try {
                    synchronized (this) {
                        wait();
                    }
                } catch (InterruptedException e) {
                    logger.error("Error during wait", e);
                }
            }
            {
                try {
                    currentRequest.setRefresh(false).execute(new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            logger.warn("Got response change bulks from {}", onGoingBulks);
                            onGoingBulks.decrementAndGet();
                            logger.warn("to {}", onGoingBulks);
                            notifyCSVRiver();
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.warn("Got response change bulks from {}", onGoingBulks);
                            onGoingBulks.decrementAndGet();
                            logger.warn("to {}", onGoingBulks);
                            notifyCSVRiver();
                            logger.warn("failed to execute bulk");
                        }
                    });
                } catch (Exception e) {
                    logger.warn("Got response change bulks from {}", onGoingBulks);
                    onGoingBulks.decrementAndGet();
                    logger.warn("to {}", onGoingBulks);
                    notifyCSVRiver();
                    logger.warn("failed to process bulk", e);
                }
            }
            currentRequest = client.prepareBulk();
        }
    }

    private void notifyCSVRiver() {
        synchronized (CSVRiver.this) {
            CSVRiver.this.notify();
        }
    }

    private class CSVConnector implements Runnable {

        @Override
        public void run() {
            while (!closed) {
                File lastProcessedFile = null;
                try {
                    File files[] = getFiles();
                    for (File file : files) {
                        logger.info("Processing file {}", file.getName());
                        file = renameFile(file, ".processing");
                        lastProcessedFile = file;

                        processFile(file);

                        processBulkIfNeeded(true);

                        while (onGoingBulks.get() > 0) {
                            logger.warn("done, ongoing bulk, [{}], waiting", onGoingBulks);
//                            Thread.sleep(500); //TODO: make this wake up
                            synchronized (CSVRiver.this) {
                                CSVRiver.this.wait();
                            }
                        }

                        file = renameFile(file, ".imported");
                        lastProcessedFile = file;
                    }
                    delay();
                } catch (Exception e) {
                    if (lastProcessedFile != null) {
                        renameFile(lastProcessedFile, ".error");
                    }
                    logger.error(e.getMessage(), e, (Object) null);
                    closed = true;
                }
                if (closed) {
                    return;
                }
            }
        }

        private File renameFile(File file, String suffix) {
            File newFile = new File(file.getAbsolutePath() + suffix);
            if (!file.renameTo(newFile)) {
                logger.error("can't rename file {} to {}", file.getName(), newFile.getName());
            }
            return newFile;
        }

        private File[] getFiles() {
            File folder = new File(folderName);
            return folder.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File file, String s) {
                    return s.matches(filenamePattern);
                }
            });
        }

        private void processFile(File file) throws IOException {
            CSVReader reader = new CSVReader(new BufferedReader(new FileReader(file)), separator, quoteCharacter, escapeCharacter);
            String[] nextLine;
            Pattern p = Pattern.compile("Performance.*", Pattern.CASE_INSENSITIVE);
            Matcher matcher = p.matcher(filenamePattern);
            Boolean performanceData = matcher.find();

            logger.info("File patter {} matches? {}", filenamePattern, performanceData);

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length > 0 && !(nextLine.length == 1 && nextLine[0].trim().equals(""))) {

                    String id = UUID.randomUUID().toString();


                    if (performanceData) {
                        Map<String, Object> holder = new HashMap<String, Object>();
                        Map<String, Object> holder2 = new HashMap<String, Object>();


                        Map<String, Object> m = new HashMap<String, Object>();

                        int position = 0;
                        for (Object fieldName : csvFields) {
                            if (fieldName.equals("Loan_Identifier")) {
                                id = nextLine[position];
                            }
                            m.put((String) fieldName, nextLine[position++]);
                        }

                        holder.put("p", m);

                        Object[] dontGetUpsert = {m};

                        holder2.put("performance", dontGetUpsert);

                        UpdateRequestBuilder urb = client.prepareUpdate(indexName, typeName, id).setScript("ctx._source.performance += p").setScriptParams(holder).setUpsert(holder2);

//                        logger.info("Query: {}", urb);
//                        logger.info("Query2: {}", urb.request().toString());
//
//                        StreamOutput outputStream = new BytesStreamOutput();
//
//                        urb.request().writeTo(outputStream);
//
//                        logger.info("Query3: {}", outputStream.toString());
//
//
//                        byte[] b = new byte[10000];
//
//                        outputStream.write(b);
//                        outputStream.flush();
//
//                        logger.info("Query4: {}", new String(b));

                        currentRequest.add(urb);

                    } else {

                        XContentBuilder builder = XContentFactory.jsonBuilder();
                        builder.startObject();

                        int position = 0;
                        for (Object fieldName : csvFields) {
                            if (fieldName.equals("Loan_Identifier")) {
                                id = nextLine[position];
                            }
                            builder.field((String) fieldName, nextLine[position++]);
                        }
                        Object[] o = {};
                        builder.field("performance", o);

                        builder.endObject();

//                        currentRequest.add(client.prepareUpdate(indexName, typeName, id).setDoc(builder).setDocAsUpsert(true));

                        currentRequest.add(Requests.indexRequest(indexName).type(typeName).id(id).create(true).source(builder));
                    }
                }
                processBulkIfNeeded(false);
            }
            processBulkIfNeeded(true);
            reader.close();
        }
    }
}
