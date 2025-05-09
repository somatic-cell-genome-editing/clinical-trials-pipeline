package edu.mcw.scge.platform;


import edu.mcw.scge.dao.implementation.ClinicalTrailDAO;
import edu.mcw.scge.datamodel.ClinicalTrialRecord;
import edu.mcw.scge.platform.index.Index;
import edu.mcw.scge.platform.index.IndexAdmin;
import edu.mcw.scge.platform.index.ProcessFile;
;


import edu.mcw.scge.platform.utils.OntologyProcessor;
import edu.mcw.scge.process.Utils;
import edu.mcw.scge.services.ESClient;

import org.apache.logging.log4j.LogManager;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;



import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;


import java.io.IOException;

import java.util.*;
import java.util.stream.Collectors;

public class Main {
    private String version;
    private IndexAdmin admin;
    private Index index;
    String command;
    String env;
    String source;
    IndexAdmin indexer=new IndexAdmin();
    private static List environments;
    ClinicalTrailDAO clinicalTrailDAO=new ClinicalTrailDAO();
    ProcessFile fileProcess=new ProcessFile();
    OntologyProcessor ontologyProcessor=new OntologyProcessor();
    protected static Logger logger= LogManager.getLogger();
    public static void main(String[] args) throws IOException {

        DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Main manager= (Main) bf.getBean("manager");
        manager.command=args[0];
        manager.env=args[1];
        manager.source=args[2];
        logger.info(manager.version);
      String index="scgeplatform_search_ct_"+manager.env;
      //  String index= SCGEContext.getESIndexName();
        List<String> indices= new ArrayList<>();
        if (environments.contains(manager.env)) {
            manager.index.setIndex( index);
            indices.add(index+ "1");
           indices.add(index  + "2");
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

    }

    public void run() throws Exception {
        long start = System.currentTimeMillis();
     //   String fileName="data/GT_tracker_release2_WIP.xlsx";
        String fileName=System.getenv("FILE_NAME");
        logger.info("FILE NAME:"+ fileName);
        if (command.equalsIgnoreCase("reindex"))
           admin.createIndex("clinicalTrialMappings", "");
        switch (source) {
            case "api" :
                /* download all data from clinical trails API and load to database. */
                    download();
                    break;
            case "file" :
                /* read from Excel sheet and directly index json string into the ES*/
                    processFile(fileName);
                    break;
            case "file1" :
                /*read NCTIDS from Excel sheet */
                List<String> nctIds= parseNCTIds(fileName);
                logger.info("NCTIDS:"+ nctIds);
                /* Query clinical trials API and load API results to database*/
                queryApiNUploadToDB(nctIds);
                /* read from Excel sheet and upload curated fields to DB  */
                processFile1(fileName);
                /*index clinical trials*/
                fileProcess.indexClinicalTrials();
                break;
            case "release2_file" :
                extractNewFieldsFromFile(fileName,"updated on STAGE" );
                fileProcess.indexClinicalTrials();
                break;
            case "update-and-index-db" :
                List<ClinicalTrialRecord> trials= clinicalTrailDAO.getAllClinicalTrailRecords();
                List<String> nctIdsFromDB= trials.stream().map(ClinicalTrialRecord::getNctId).collect(Collectors.toList());
                /* Query clinical trials API and load API results to database*/
                queryApiNUploadToDB(nctIdsFromDB);
                /* read from Excel sheet and upload curated fields to DB  */
//                processFile1(fileName);
                /*index clinical trials*/
                fileProcess.indexClinicalTrials();
                break;
            case "index-only-from-db" :
                /*index clinical trials*/
                fileProcess.indexClinicalTrials();
                break;
            case "update-ontology-terms" :
                ontologyProcessor.uploadParentTerms();
                fileProcess.indexClinicalTrials();
                break;
            default :
        }

        String clusterStatus = this.getClusterHealth(Index.getNewAlias());
        if (!clusterStatus.equalsIgnoreCase("ok")) {
            logger.info(clusterStatus + ", refusing to continue with operations");
        } else {
            if (command.equalsIgnoreCase("reindex")) {
                logger.info("CLUSTER STATUR:" + clusterStatus + ". Switching Alias...");
                switchAlias();
            }
        }
        long end = System.currentTimeMillis();
        logger.info(" - " + Utils.formatElapsedTime(start, end));
        logger.info("CLIENT IS CLOSED");
    }
    public List<String> getIndicationDOIDs() throws Exception {
        List<ClinicalTrialRecord> records=clinicalTrailDAO.getAllClinicalTrailRecords();
        return records.stream().map(ClinicalTrialRecord::getIndicationDOID).collect(Collectors.toList());
    }
    public void processFile(String filename) throws Exception {
        ProcessFile fileProcess=new ProcessFile();
        fileProcess.indexFromFile(filename);
    }
    public void processFile1(String filename) throws Exception {
        ProcessFile fileProcess=new ProcessFile();
        fileProcess.parseFileNLoadToDB(filename);
    }
    public void extractNewFieldsFromFile(String filename, String sheet) throws Exception {
        ProcessFile fileProcess=new ProcessFile();
        fileProcess.parseFileFields(filename,sheet);
    }

    public List<String> parseNCTIds(String filename) throws Exception {
        return fileProcess.parseFileForNCTIds(filename);
    }
    public void queryApiNUploadToDB(List<String> nctIds) throws Exception {
        clinicalTrailDAO.downloadClinicalTrails(nctIds);
        logger.info("Download from API and Upload to DB is DONE!!");
    }

    public boolean existsRecord(String nctId) throws Exception {
       List<ClinicalTrialRecord> records= clinicalTrailDAO.getClinicalTrailRecordByNctId(nctId);
       return records.size() > 0;
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


//        String nextPageToken=null;
//        do {
//            String fetchUri=null;
//            RestClient restClient = RestClient.builder()
//                    .requestFactory(new HttpComponentsClientHttpRequestFactory())
//                    .baseUrl("https://clinicaltrials.gov/api/v2")
//                    .build();
//            if(nextPageToken!=null){
//               fetchUri=baseURI+"&pageToken="+nextPageToken;
//            }else fetchUri=baseURI;
//            try {
//                String responseStr = restClient.get()
//                        .uri(fetchUri)
//                        .retrieve()
//                        .body(String.class);
//                if (responseStr != null) {
//                    JSONObject jsonObject = new JSONObject(responseStr);
//                    JSONArray array = (JSONArray) jsonObject.get("studies");
//                    for (Object object : array) {
//                        //    System.out.println(object);
//                        indexer.indexDocuments(object);
//                    }
//                    try {
//                        nextPageToken = jsonObject.get("nextPageToken") != null ? (String) jsonObject.get("nextPageToken") : null;
//                    } catch (Exception e) {
//                        nextPageToken = null;
//                        e.printStackTrace();
//                    }
//
//
//                }
//            }catch (Exception exception){
//                exception.printStackTrace();
//            }
//        }while(nextPageToken!=null);
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
        logger.info("CLUSTER STATE: " +response.getStatus().name());
        //     log.info("CLUSTER STATE: " + response.getStatus().name());
        if (response.isTimedOut()) {
            return   "cluster state is " + response.getStatus().name();
        }

        return "OK";
    }
    public boolean switchAlias() throws Exception {
        logger.info("NEEW ALIAS: " + Index.getNewAlias() + " || OLD ALIAS:" + Index.getOldAlias());
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

    public String getVersion() {
        return version;
    }
}