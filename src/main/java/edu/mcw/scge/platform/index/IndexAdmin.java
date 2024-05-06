package edu.mcw.scge.platform.index;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.scge.services.es.ESClient;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;


import java.io.IOException;
import java.util.Date;

public class IndexAdmin {

    private Index index;
    public void createIndex(String mappings, String type) throws Exception {
        GetAliasesRequest aliasesRequest = new GetAliasesRequest(index.getIndex());
        boolean existsAlias = ESClient.getClient().indices().existsAlias(aliasesRequest, RequestOptions.DEFAULT);
        if (existsAlias) {
            for (String index : Index.getIndices()) {
                aliasesRequest.indices(index);
                existsAlias = ESClient.getClient().indices().existsAlias(aliasesRequest, RequestOptions.DEFAULT);
                if (!existsAlias) {
                    Index.setNewAlias(index);
                    GetIndexRequest request1 = new GetIndexRequest(index);
                    boolean indexExists = ESClient.getClient().indices().exists(request1, RequestOptions.DEFAULT);

                    if (indexExists) {   /**** delete index if exists ****/

                        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
                        ESClient.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                        System.out.println(index + " deleted");
                    }
                    createNewIndex(index, mappings, type);
                } else {
                    Index.setOldAlias(index);
                }

            }
        } else {
            GetIndexRequest request1 = new GetIndexRequest(Index.getIndex() + "1");
            boolean indexExists = ESClient.getClient().indices().exists(request1, RequestOptions.DEFAULT);
            if (indexExists) {   /**** delete index if exists ****/

                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(Index.getIndex() + "1");
                ESClient.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                System.out.println(Index.getIndex() + "1" + " deleted");
            }
            createNewIndex(Index.getIndex() + "1", mappings, type);

        }
    }
        public void createNewIndex(String index, String _mappings, String type) throws Exception {

         //   String path= "data/indexDocMappings.json";
            System.out.println("CREATING NEW INDEX..." + index);

          //  String mappings=new String(Files.readAllBytes(Paths.get(path)));
         //   String analyzers=new String(Files.readAllBytes(Paths.get("data/analyzers.json")));
            int replicates=0;
            if(!index.contains("dev")){
                replicates=1;
            }
            /********* create index, put mappings and analyzers ****/
            CreateIndexRequest request=new CreateIndexRequest(index);
            request.settings(Settings.builder()
                    .put("index.number_of_shards",5)
                    .put("index.number_of_replicas", replicates))
               //     .loadFromSource(analyzers, XContentType.JSON))
            ;
//            request.mapping(mappings,
//                    XContentType.JSON);
            org.elasticsearch.client.indices.CreateIndexResponse createIndexResponse = ESClient.getClient().indices().create(request, RequestOptions.DEFAULT);
            System.out.println(index + " created on  " + new Date());

            Index.setNewAlias(index);

    }
    public void indexDocuments(Object str) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        //    bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //  bulkRequest.timeout(TimeValue.timeValueMinutes(2));
        //  bulkRequest.timeout("2m");


        try {
            bulkRequest.add(new IndexRequest(Index.getNewAlias()).source(str.toString(), XContentType.JSON));
        } catch (Exception e) {
            e.printStackTrace();
        }
        //     bulkRequestBuilder.add(new IndexRequest(index, type,o.getTerm_acc()).source(json, XContentType.JSON));


        ESClient.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        RefreshRequest refreshRequest = new RefreshRequest();
        ESClient.getClient().indices().refresh(refreshRequest, RequestOptions.DEFAULT);

    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }
}
