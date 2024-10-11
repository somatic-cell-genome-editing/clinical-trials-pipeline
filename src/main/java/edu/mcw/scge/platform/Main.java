package edu.mcw.scge.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.scge.dao.implementation.ClinicalTrailDAO;
import edu.mcw.scge.datamodel.ClinicalTrialRecord;
import edu.mcw.scge.platform.index.Index;
import edu.mcw.scge.platform.index.IndexAdmin;
import edu.mcw.scge.platform.index.ProcessFile;
import edu.mcw.scge.platform.model.*;


import edu.mcw.scge.process.Utils;
import edu.mcw.scge.services.ESClient;
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
    public static void main(String[] args) throws IOException {
        DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Main manager= (Main) bf.getBean("manager");
        manager.command=args[0];
        manager.env=args[1];
        manager.source=args[2];

     //   String index="scge_platform_search";
        String index="scge_platform_ctapi_search";
        List<String> indices= new ArrayList<>();
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
        switch (source) {
            case "api" ->
                /* download all data from clinical trails API and load to database. */
                    download();
            case "file" ->
                /* read from Excel sheet and directly index json string into the ES*/
                    processFile("data/GT_tracker_with_sources.xlsx");
            case "file1" -> {
                /*read NCTIDS from Excel sheet */
              List<String> nctIds= parseNCTIds("data/GT_tracker_with_sources.xlsx");
              /*Query clinical trials API and load API results to database*/
                queryApiNUploadToDB(nctIds);
                /* read from Excel sheet and upload curated field to DB and index */
                processFile1("data/GT_tracker_with_sources.xlsx");
            }
               default -> {
            }
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
    public void processFile1(String filename) throws Exception {
        ProcessFile fileProcess=new ProcessFile();
        fileProcess.uploadNIndexFromFile(filename);
    }
    public List<String> parseNCTIds(String filename) throws Exception {
        return fileProcess.parseFileForNCTIds(filename);
    }
    public void queryApiNUploadToDB(List<String> nctIds) throws Exception {

      //  List<String> nctIds=fileProcess.parseFileForNCTIds(filename);
       // uploadClinicalTrails(nctIds);
        clinicalTrailDAO.downloadClinicalTrails(nctIds);
     //   fileProcess.indexClinicalTrials();
        System.out.println("DONE!!");
    }

    public boolean existsRecord(String nctId) throws Exception {
       List<ClinicalTrialRecord> records= clinicalTrailDAO.getClinicalTrailRecordByNctId(nctId);
       return records.size() > 0;
    }
    public void uploadClinicalTrails(List<String> nctIds) throws Exception {
        if(nctIds.size()>0) {
            String baseURI="https://clinicaltrials.gov/api/v2/studies/";

            RestClient restClient = RestClient.builder()
                    .requestFactory(new HttpComponentsClientHttpRequestFactory())
                    .baseUrl("https://clinicaltrials.gov/api/v2")
                    .build();
            ObjectMapper mapper= new ObjectMapper();
            loop:  for (String nctId : nctIds) {
                if (nctId == null || nctId.equals("") || nctId.equals("null"))
                    continue loop;
                //  String nctId="NCT02852213";
                if (existsRecord(nctId)) {
                   // clinicalTrailDAO.updateClinicalTrailRecord(nctId);
                } else {
                    String fetchUri = baseURI + nctId;
                    try {
                        String responseStr = restClient.get()
                                .uri(fetchUri)
                                .retrieve()
                                .body(String.class);
                        if (responseStr != null) {
                            JSONObject jsonObject = new JSONObject(responseStr);

                            //  System.out.println(jsonObject);

                            Study study = mapper.readValue(jsonObject.toString(), Study.class);
                            ClinicalTrialRecord record = new ClinicalTrialRecord();
                            record.setNctId(nctId);
                            record.setDescription(study.getProtocolSection().getDescriptionModule().getBriefSummary());
                            StringBuilder interventions = new StringBuilder();
                            StringBuilder interventionDescription = new StringBuilder();

                            // System.out.println("Interventions");
                            for (Intervention intervention : study.getProtocolSection().getArmsInterventionsModule().getInterventions()) {
                                Map<String, Object> otherProps = intervention.getAdditionalProperties();
                                interventions.append(intervention.getName());
                                interventions.append(", ");
                                interventionDescription.append(otherProps.get("description"));
                                //  System.out.print(intervention.getName()+"\tOtherName:"+otherProps.get("otherNames")+"\tDosage:"+otherProps.get("description")+"\n");
                            }
                            record.setInterventionName(interventions.toString());
                            record.setInterventionDescription(interventionDescription.toString());
                            //   System.out.println("Sponsor:"+study.getProtocolSection().getSponsorCollaboratorsModule().getLeadSponsor().getName()+"\tCLASS:"+study.getProtocolSection().getSponsorCollaboratorsModule().getLeadSponsor().getClass_());
                            record.setSponsor(study.getProtocolSection().getSponsorCollaboratorsModule().getLeadSponsor().getName());
                            record.setSponsorClass(study.getProtocolSection().getSponsorCollaboratorsModule().getLeadSponsor().getClass_());
                            //   System.out.println("Indication:"+ study.getProtocolSection().getConditionsModule().getConditions()+"\tKEYWORDS:"+study.getProtocolSection().getConditionsModule().getAdditionalProperties().get("keywords") );
                            record.setIndication(String.join(", ", study.getProtocolSection().getConditionsModule().getConditions()));
                            ArrayList<String> conditionKeywords = (ArrayList<String>) study.getProtocolSection().getConditionsModule().getAdditionalProperties().get("keywords");
                            if (conditionKeywords != null && !conditionKeywords.isEmpty())
                                record.setBrowseConditionTerms(conditionKeywords.stream().collect(Collectors.joining(", ")));
                            //    System.out.println("Phases:"+study.getProtocolSection().getDesignModule().getPhases()+"\tEnrollmentCount:"+study.getProtocolSection().getDesignModule().getEnrollmentInfo().getCount());
                            record.setPhases(String.join(", ", study.getProtocolSection().getDesignModule().getPhases()));
                            record.setEnrorllmentCount(study.getProtocolSection().getDesignModule().getEnrollmentInfo().getCount());

                            //   indexer.indexDocuments(object);
                            //   System.out.println("locations:"+ study.getProtocolSection().getContactsLocationsModule().getLocations().size());
                            if (study.getProtocolSection().getContactsLocationsModule() != null) {
                                record.setLocations(String.join(",", study.getProtocolSection().getContactsLocationsModule().getLocations().stream().map(Location::getCountry).collect(Collectors.toSet())));
                                record.setNumberOfLocations(study.getProtocolSection().getContactsLocationsModule().getLocations().size());
                            }
//                        System.out.print("Centers in USA:");
//                        for(Location location:study.getProtocolSection().getContactsLocationsModule().getLocations()){
//                            if(location.getCountry().equalsIgnoreCase("United States")){
//                                System.out.print("Yes"+"\t");
//                            }
//                        }
//                        System.out.println("\n");
//                        System.out.println("Eligibility: \tSEX:"+study.getProtocolSection().getEligibilityModule().getSex()+"\tMin AGE:"+ study.getProtocolSection().getEligibilityModule().getMinimumAge()+"\t" +
//                                "MAX AGE:"+study.getProtocolSection().getEligibilityModule().getMaximumAge() +"\tHEALTHY VOLUNTEERS:"+ study.getProtocolSection().getEligibilityModule().getHealthyVolunteers()+"\n" +
//                                "Standard Age:"+ study.getProtocolSection().getEligibilityModule().getStdAges());

                            record.setEligibilitySex(study.getProtocolSection().getEligibilityModule().getSex());
                            record.setElibilityMinAge(study.getProtocolSection().getEligibilityModule().getMinimumAge());
                            record.setElibilityMaxAge(study.getProtocolSection().getEligibilityModule().getMaximumAge());
                            record.setHealthyVolunteers(study.getProtocolSection().getEligibilityModule().getHealthyVolunteers().toString());
                            record.setStandardAges(String.join(",", study.getProtocolSection().getEligibilityModule().getStdAges()));


                            record.setIsFDARegulated(String.valueOf(study.getProtocolSection().getOversightModule().getIsFdaRegulatedDrug()));
                            record.setBriefTitle(study.getProtocolSection().getIdentificationModule().getBriefTitle());
                            record.setOfficialTitle(study.getProtocolSection().getIdentificationModule().getOfficialTitle());
                            if (study.getProtocolSection().getIdentificationModule().getAdditionalProperties().get("secondaryIdInfos") != null) {
                                ArrayList object = (ArrayList) study.getProtocolSection().getIdentificationModule().getAdditionalProperties().get("secondaryIdInfos");
                                StringBuilder builder = new StringBuilder();
                                for (Object o : object) {
                                    String link = ((Map<String, String>) o).get("link");
                                    if (link != null)
                                        builder.append(link).append(";");

                                }

                                record.setNihReportLink(builder.toString());
                            }
                            record.setStatus(study.getProtocolSection().getStatusModule().getOverallStatus());
                            record.setFirstSubmitDate((study.getProtocolSection().getStatusModule().getStudyFirstSubmitDate()));
                            record.setEstimatedCompleteDate((study.getProtocolSection().getStatusModule().getCompletionDateStruct().getDate()));
                            record.setLastUpdatePostDate((study.getProtocolSection().getStatusModule().getLastUpdatePostDateStruct().getDate()));
//                        System.out.println("Derived Section Browse Branches:");
//                        for(BrowseBranch branch:study.getDerivedSection().getConditionBrowseModule().getBrowseBranches()){
//                            System.out.print(branch.getName()+",\t");
//                        }
//                        System.out.println("\nDerived Section Browse Leaf:");
//                        for(Browseleaf branch:study.getDerivedSection().getConditionBrowseModule().getBrowseLeaves()){
//                            System.out.print(branch.getName()+",\t");
//                        }
//                        System.out.println("\nDerived Section Browse Ancestors:");
//                        for(Ancestor branch:study.getDerivedSection().getConditionBrowseModule().getAncestors()){
//                            System.out.print(branch.getTerm()+",\t");
//                        }
//                        System.out.println("\nDerived Section Browse MEshes:");
//                        for(Mesh branch:study.getDerivedSection().getConditionBrowseModule().getMeshes()){
//                            System.out.print(branch.getTerm()+",\t");
//                        }
//
//                        System.out.println("Derived Section Browse intervention Branches:");
//                        for(Map.Entry branch:((Map<String, Object>)study.getDerivedSection().getAdditionalProperties().get("interventionBrowseModule")).entrySet()){
//                            System.out.print(branch.getValue()+",\t");
//                        }
                            clinicalTrailDAO.insert(record);
                        }
                    } catch (Exception exception) {
                        System.out.println("NCTID:" + nctId);
                        exception.printStackTrace();
                    }
                }
            }
        }
    }
//    public void indexClinicalTrails() throws Exception {
//        List<ClinicalTrialCuratedData> records=clinicalTrailDAO.getAllClinicalTrailRecords();
//        if(records.size()>0){
//            for(ClinicalTrialRecord record:records){
//                fileProcess.indexClinicalTrailRecord(record);
//            };
//        }
//    }
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