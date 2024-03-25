package edu.mcw.scge.platform;

import edu.mcw.scge.platform.index.Index;
import edu.mcw.scge.platform.index.Indexer;
import edu.mcw.scge.process.Utils;
import edu.mcw.scge.services.es.ESClient;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private String version;
    private Indexer admin;
    private Index index;
    String command;
    String env;
    Indexer indexer=new Indexer();
    private static List environments;

    public static void main(String[] args) throws IOException {
        DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Main manager= (Main) bf.getBean("manager");
        manager.command=args[0];
        manager.env=args[1];
        String index="scge_platform_search";
        List<String> indices= new ArrayList<>();
    //    ESClient es= (ESClient) bf.getBean("client");
        if (environments.contains(manager.env)) {
            manager.index.setIndex( index +"_"+manager.env);
            indices.add(index+"_"+manager.env + "1");
            indices.add(index + "_"+manager.env + "2");
           manager.index.setIndices(indices);
        }
        manager.index= (Index) bf.getBean("index");
        try {
            manager.run();
        } catch (Exception e) {

                try {
                    ESClient.destroy();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

            e.printStackTrace();
        }
            ESClient.destroy();
        System.out.println(manager.version);
    }

    public void run() throws Exception {
        long start = System.currentTimeMillis();
        if (command.equalsIgnoreCase("reindex"))
           admin.createIndex("", "");
        download();
        String clusterStatus = this.getClusterHealth(Index.getNewAlias());
        if (!clusterStatus.equalsIgnoreCase("ok")) {
            System.out.println(clusterStatus + ", refusing to continue with operations");
        } else {
            if (command.equalsIgnoreCase("reindex")) {
                System.out.println("CLUSTER STATUR:" + clusterStatus + ". Switching Alias...");
                switchAlias();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(" - " + Utils.formatElapsedTime(start, end));
        System.out.println("CLIENT IS CLOSED");
    }
    public void download() throws IOException {

        String baseURI="https://clinicaltrials.gov/api/v2/studies?pageSize=1&countTotal=true&query.term=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true";
        String nextPageToken=null;
        do {
            String fetchUri=null;
            RestClient restClient = RestClient.builder()
                    .requestFactory(new HttpComponentsClientHttpRequestFactory())
                    .baseUrl("https://clinicaltrials.gov/api/v2")
                    .build();
            if(nextPageToken!=null){
               fetchUri=baseURI+"&pageToken="+nextPageToken;
            }else fetchUri=baseURI;
            String responseStr = restClient.get()
                    .uri(fetchUri)
                    .retrieve()
                    .body(String.class);
            if(responseStr!=null) {
           //     JSONObject jsonObject = new JSONObject(responseStr);
           //     nextPageToken =jsonObject.get("nextPageToken")!=null? (String) jsonObject.get("nextPageToken"):null;

           //     System.out.println(jsonObject.get("studies"));
            indexer.indexDocuments(responseStr);
            }
        }while(nextPageToken!=null);
    }
    public void setVersion(String version) {
    }


    public void setIndexName(Index indexName) {
    }


    public Indexer getAdmin() {
        return admin;
    }

    public void setAdmin(Indexer admin) {
        this.admin = admin;
    }



    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }
    

    public String getClusterHealth(String index) throws Exception {

        ClusterHealthRequest request = new ClusterHealthRequest(index);
        ClusterHealthResponse response = ESClient.getClient().cluster().health(request, RequestOptions.DEFAULT);
        System.out.println(response.getStatus().name());
        //     log.info("CLUSTER STATE: " + response.getStatus().name());
        if (response.isTimedOut()) {
            return   "cluster state is " + response.getStatus().name();
        }

        return "OK";
    }
    public boolean switchAlias() throws Exception {
        System.out.println("NEEW ALIAS: " + Index.getNewAlias() + " || OLD ALIAS:" + Index.getOldAlias());
        IndicesAliasesRequest request = new IndicesAliasesRequest();


        if (Index.getOldAlias() != null) {

            IndicesAliasesRequest.AliasActions removeAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                            .index(Index.getOldAlias())
                            .alias(Index.getIndex());
            IndicesAliasesRequest.AliasActions addAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(Index.getNewAlias())
                            .alias(Index.getIndex());
            request.addAliasAction(removeAliasAction);
            request.addAliasAction(addAliasAction);
            //    log.info("Switched from " + RgdIndex.getOldAlias() + " to  " + RgdIndex.getNewAlias());

        }else{
            IndicesAliasesRequest.AliasActions addAliasAction =
                    new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(Index.getNewAlias())
                            .alias(Index.getIndex());
            request.addAliasAction(addAliasAction);
            //    log.info(rgdIndex.getIndex() + " pointed to " + RgdIndex.getNewAlias());
        }
        AcknowledgedResponse indicesAliasesResponse =
                ESClient.getClient().indices().updateAliases(request, RequestOptions.DEFAULT);

        return  true;

    }


    public void setEnvironments(List environments) {
        this.environments = environments;
    }

    public List getEnvironments() {
        return environments;
    }
}