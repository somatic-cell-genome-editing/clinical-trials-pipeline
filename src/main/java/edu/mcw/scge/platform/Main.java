package edu.mcw.scge.platform;


import edu.mcw.scge.dao.implementation.ClinicalTrailDAO;
import edu.mcw.scge.datamodel.ClinicalTrialRecord;


import edu.mcw.scge.platform.utils.OntologyProcessor;
import edu.mcw.scge.process.Utils;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    String source;


    ClinicalTrailDAO clinicalTrailDAO=new ClinicalTrailDAO();
    ProcessFile fileProcess=new ProcessFile();
    OntologyProcessor ontologyProcessor=new OntologyProcessor();
    protected static Logger logger= LogManager.getLogger();
    public static void main(String[] args) throws IOException {

        DefaultListableBeanFactory bf= new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf) .loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Main manager= (Main) bf.getBean("manager");

        manager.source=args[0];



        try {
            manager.run();
        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    public void run() throws Exception {
        long start = System.currentTimeMillis();
     //   String fileName="data/GT_tracker_release2_WIP.xlsx";
        String fileName=System.getenv("FILE_NAME");
        logger.info("FILE NAME:"+ fileName);

        switch (source) {
            case "api" :
                /* download all data from clinical trails API and load to database. */
                    download();
                    break;

            case "file1" :
                /*read NCTIDS from Excel sheet */
                List<String> nctIds= parseNCTIds(fileName);
                logger.info("NCTIDS:"+ nctIds);
                /* Query clinical trials API and load API results to database*/
                queryApiNUploadToDB(nctIds);
                /* read from Excel sheet and upload curated fields to DB  */
                processFile1(fileName);

                break;
            case "release2_file" :
                extractNewFieldsFromFile(fileName,"updated on STAGE" );

                break;
            case "update-db" :
                List<ClinicalTrialRecord> trials= clinicalTrailDAO.getAllClinicalTrailRecords();
                List<String> nctIdsFromDB= trials.stream().map(ClinicalTrialRecord::getNctId).collect(Collectors.toList());
                /* Query clinical trials API and load API results to database*/
                queryApiNUploadToDB(nctIdsFromDB);
                /* read from Excel sheet and upload curated fields to DB  */
//                processFile1(fileName);

                break;

            case "update-ontology-terms" :
                ontologyProcessor.uploadParentTerms();
                break;
            default :
        }


        long end = System.currentTimeMillis();
        logger.info(" - " + Utils.formatElapsedTime(start, end));
    }
    public List<String> getIndicationDOIDs() throws Exception {
        List<ClinicalTrialRecord> records=clinicalTrailDAO.getAllClinicalTrailRecords();
        return records.stream().map(ClinicalTrialRecord::getIndicationDOID).collect(Collectors.toList());
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
        String baseURI="https://clinicaltrials.gov/api/v2/studies?countTotal=true" +
                "&query.term=" +
                "AREA[protocolSection.descriptionModule.briefSummary](gene therapy OR gene transfer OR gene editing OR CRISPR OR CAR-T OR CAR T-cell OR cell therapy OR antisense OR siRNA OR mRNA therapy OR oligonucleotide) OR " +
                "AREA[protocolSection.descriptionModule.detailedDescription](gene therapy OR gene transfer OR gene editing OR CRISPR OR CAR-T) OR " +
                "AREA[protocolSection.identificationModule.officialTitle](gene therapy OR gene transfer OR gene editing OR AAV OR CRISPR OR CAR-T) OR " +
                "AREA[InterventionSearch](gene therapy OR gene transfer OR gene editing OR CRISPR OR CAR-T) OR " +
                "AREA[ConditionSearch](gene therapy OR gene transfer OR gene editing)" +
                "&filter.advanced=AREA[protocolSection.oversightModule.isFdaRegulatedDrug]true"+
                "&filter.advanced=AREA[LastUpdatePostDate]RANGE[2023-01-01, MAX]";


        Set<String> nctIds=new HashSet<>();
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
                JSONObject jsonObject = new JSONObject(responseStr);
                JSONArray array =  jsonObject.getJSONArray("studies");
                for (int i=0;i<array.length();i++) {
                    JSONObject o = array.getJSONObject(i);
                    JSONObject protocolSection = o.getJSONObject("protocolSection");
                    JSONObject identificationModule = protocolSection.getJSONObject("identificationModule");
                    String nctId=identificationModule.getString("nctId");
                    if(nctId!=null && !nctId.equals("")){

                        clinicalTrailDAO.insertClinicalTrialAPIObject(o.toString(), nctId, "api");
                        nctIds.add(nctId);}

                }
                try {
                    nextPageToken = jsonObject.get("nextPageToken") != null ? (String) jsonObject.get("nextPageToken") : null;
                } catch (Exception e) {
                    nextPageToken = null;
                    e.printStackTrace();
                }


            }catch (Exception exception){
                exception.printStackTrace();
            }
        }while(nextPageToken!=null);
        System.out.println("NCTIDS:"+ nctIds.size());
    }
    public void setVersion(String version) {
    }

}