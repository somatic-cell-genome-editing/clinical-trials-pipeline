package edu.mcw.scge.platform;

import edu.mcw.scge.platform.index.Index;
import edu.mcw.scge.platform.index.IndexAdmin;
import edu.mcw.scge.platform.index.ProcessFile;
import edu.mcw.scge.platform.process.Utils;
import edu.mcw.scge.platform.services.ESClient;

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
    private IndexAdmin admin;
    private Index index;
    String command;
    String env;
    String source;
    IndexAdmin indexer=new IndexAdmin();
    private static List environments;

    public static void main(String[] args) throws IOException {
        DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Main manager= (Main) bf.getBean("manager");
        manager.command=args[0];
        manager.env=args[1];
        manager.source=args[2];
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
        switch (source){
            case  "api":
                download();
                break;
            case "file":
                processFile("data/GT_tracker_050124.xlsx");
                break;
            default:

        }

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
    public void processFile(String filename) throws Exception {
        ProcessFile fileProcess=new ProcessFile();
        fileProcess.indexFromFile(filename);
    }
    public void download() throws IOException {

     //   String baseURI="https://clinicaltrials.gov/api/v2/studies?pageSize=10&countTotal=true&query.term=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true&query.intr=Gene+Therapy";
     //  String baseURI="https://clinicaltrials.gov/api/v2/studies?query.cond=(gene+therapy+OR+gene+editing)&query.intr=BIOLOGICAL&postFilter.advanced=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true&countTotal=true";
//       String baseURI="https://clinicaltrials.gov/api/v2/studies?query.cond=(Gene Therapy OR Gene Editing OR GENETIC OR BIOLOGICAL)&query.intr=(BIOLOGICAL OR GENETIC OR GENE THERAPY OR GENE EDITING)&filter.advanced=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true&query.term=(Gene Therapy OR Gene Editing OR GENETIC OR BIOLOGICAL)&countTotal=true"
////               "&postFilter.advanced=AREA[LastUpdatePostDate]RANGE[2023-01-15,MAX]D"
//              ;

//        String baseURI="https://clinicaltrials.gov/api/v2/studies?query.cond=AREA[ConditionSearch](Gene Therapy OR Gene Editing OR GENETIC OR BIOLOGICAL)&query.term=AREA[BasicSearch](Gene Therapy OR Gene Editing OR GENETIC OR BIOLOGICAL) OR AREA[protocolSection.descriptionModule.detailedDescription](Gene Therapy, Gene Edit)&query.intr=AREA[InterventionSearch](GENETIC OR BIOLOGICAL OR Gene Therapy OR Gene Editing)" +
//                "&postFilter.advanced=AREA[LastUpdatePostDate]RANGE[2023-01-15,2024-04-23] AND AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true" +
//                "&countTotal=true";
        //&filter.advanced=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true
//        String baseURI="https://clinicaltrials.gov/api/v2/studies?query.intr=AREA[InterventionSearch](GENETIC OR BIOLOGICAL OR Gene Therapy OR Gene Editing)" +
//                "&postFilter.advanced=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true" +
//                "&query.term=AREA[LastUpdatePostDate]RANGE[2023-01-15,2024-04-23]" +
//                "&countTotal=true";
//        String baseURI="https://clinicaltrials.gov/api/v2/studies?countTotal=true&query.intr=Gene+Therapy" +
//                "&query.term=gene+therapy" +
//                "&filter.advanced=+AREA[LastUpdatePostDate]RANGE[2023-01-01, MAX]";
        String baseURI="https://clinicaltrials.gov/api/v2/studies?countTotal=true" ;
//                "&query.term=AREA[protocolSection.descriptionModule.briefSummary]gene therapy OR AREA[InterventionSearch]gene therapy OR AREA[InterventionSearch]gene transfer OR AREA[InterventionSearch]gene editing" +
//                " OR AREA[EligibilitySearch]gene therapy OR AREA[EligibilitySearch]gene transfer OR AREA[EligibilitySearch]gene editing OR AREA[ConditionSearch]gene transfer OR AREA[ConditionSearch]gene editing "
//              +  "&postFilter.advanced=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true"+
//
//                "&filter.advanced=+AREA[LastUpdatePostDate]RANGE[2023-01-01, MAX]";


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
            try {
                String responseStr = restClient.get()
                        .uri(fetchUri)
                        .retrieve()
                        .body(String.class);
                if (responseStr != null) {
                    JSONObject jsonObject = new JSONObject(responseStr);
                    JSONArray array = (JSONArray) jsonObject.get("studies");
                    for (Object object : array) {
                        //    System.out.println(object);
                        indexer.indexDocuments(object);
                    }
                    try {
                        nextPageToken = jsonObject.get("nextPageToken") != null ? (String) jsonObject.get("nextPageToken") : null;
                    } catch (Exception e) {
                        nextPageToken = null;
                        e.printStackTrace();
                    }


                }
            }catch (Exception exception){
                exception.printStackTrace();
            }
        }while(nextPageToken!=null);
    }
    public void setVersion(String version) {
    }


    public void setIndexName(Index indexName) {
    }


    public IndexAdmin getAdmin() {
        return admin;
    }

    public void setAdmin(IndexAdmin admin) {
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