package fr.dzinsou.yarnmonitoring.output;

import fr.dzinsou.yarnmonitoring.AppConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ElasticsearchOutput extends AbstractOutput {
    private final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchOutput.class);

    private AppConfig appConfig;

    private RestHighLevelClient client;

    private DateTimeFormatter dateTimeFormatter;

    private long totalAsyncRequestSent = 0L;
    private long totalAsyncRequestFinished = 0L;

    public ElasticsearchOutput(AppConfig appConfig) throws IOException {
        this.appConfig = appConfig;
        init();
    }

    private void init() throws IOException {
        String hostname = appConfig.getOutputElasticsearchHttpHostArray()[0];
        int port = appConfig.getOutputElasticsearchHttpPort();
        String scheme = appConfig.getOutputElasticsearchHttpScheme();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, scheme)));
        dateTimeFormatter = DateTimeFormatter.ofPattern(appConfig.getOutputElasticsearchIndexPattern());
    }

    @Override
    public void bulkSave(List<String> idList, List<String> partitionIdList, List<String> jsonRecordList, boolean async) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < idList.size(); i++) {
            String id = idList.get(i);
            String jsonRecord = jsonRecordList.get(i);
            IndexRequest indexRequest = new IndexRequest(getIndex());
            if (id != null) {
                indexRequest.id(id);
            }
            if (appConfig.getOutputElasticsearchDocType() != null && !appConfig.getOutputElasticsearchDocType().equals("")) {
                indexRequest.type(appConfig.getOutputElasticsearchDocType());
            }
            indexRequest.source(jsonRecord, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        bulkRequest.timeout(appConfig.getOutputElasticsearchRequestTimeout());

        if (async) {
            client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    totalAsyncRequestFinished++;
                    if (bulkResponse.hasFailures()) {
                        LOGGER.error("Elasticsearch bulk failures: [{}]", bulkResponse.buildFailureMessage());
                    } else {
                        LOGGER.debug("Elasticsearch bulk success");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    totalAsyncRequestFinished++;
                    LOGGER.error(e.getMessage(), e);
                }
            });
        } else {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        }
        totalAsyncRequestSent++;
    }

    @Override
    public void close() throws IOException {
        do {
            LOGGER.info("Waiting for pending requests: [{}]/[{}]", totalAsyncRequestFinished, totalAsyncRequestSent);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        } while (totalAsyncRequestSent != totalAsyncRequestFinished);

        LOGGER.info("Pending requests check: [{}]/[{}]", totalAsyncRequestFinished, totalAsyncRequestSent);
        LOGGER.info("Closing Elasticsearch connection");
        client.close();
    }

    private String getIndex() {
        return dateTimeFormatter.format(LocalDateTime.now());
    }
}
