package edu.mcw.scge.platform.index;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.gson.Gson;
import edu.mcw.scge.dao.implementation.ClinicalTrailDAO;
import edu.mcw.scge.datamodel.Alias;
import edu.mcw.scge.datamodel.ClinicalTrialAdditionalInfo;
import edu.mcw.scge.datamodel.ClinicalTrialExternalLink;
import edu.mcw.scge.datamodel.ClinicalTrialRecord;
import edu.mcw.scge.platform.model.platform.AliasType;
import edu.mcw.scge.platform.utils.BulkIndexProcessor;
import edu.mcw.scge.services.ESClient;
import edu.mcw.scge.services.SCGEContext;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.Cell;

import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.xcontent.XContentType;
import org.json.JSONArray;
import org.json.JSONObject;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class ProcessFile {
    ClinicalTrailDAO clinicalTrailDAO=new ClinicalTrailDAO();
    Gson gson=new Gson();
    ObjectMapper mapper=new ObjectMapper();
    public void parseFileFields(String file, String sheetName) throws Exception {
        FileInputStream fs=new FileInputStream(new File(file));
        XSSFWorkbook workbook=new XSSFWorkbook(fs);
        XSSFSheet sheet=workbook.getSheet(sheetName);
        if(sheet==null){
            throw new Exception("Sheet is null");
        }
        ObjectMapper mapper=JsonMapper.builder().
                enable( JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER).build();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);

          for (Row row : sheet) {

            if (row.getRowNum() >4) {
                String NCTNumber= String.valueOf(row.getCell(0));
                Iterator<Cell> cellIterator=row.cellIterator();
                if( NCTNumber==null || NCTNumber.trim().isEmpty() || NCTNumber.equals("null") || NCTNumber.equals("")){
                    continue;
                }
                ClinicalTrialRecord record=new ClinicalTrialRecord();
                List<ClinicalTrialExternalLink> externalLinks=new ArrayList<>();
                List<Alias> aliases=new ArrayList<>();
                List<ClinicalTrialAdditionalInfo> info=new ArrayList<>();
                while (cellIterator.hasNext()) {

                    Cell cell = cellIterator.next();
                    int colIndex = cell.getColumnIndex();

                    if(colIndex==0) {
                        if (row.getCell(colIndex) != null && !row.getCell(colIndex).toString().isEmpty()) {
                            String     columnVal = String.valueOf(row.getCell(colIndex));
                            record.setNctId(columnVal);
                        }
                    }
                    if(colIndex==1) {
                        if (row.getCell(colIndex) != null && !row.getCell(colIndex).toString().isEmpty()) {
                            String  columnVal = String.valueOf(row.getCell(colIndex));
                            record.setDevelopmentStatus(columnVal);
                        }
                    }
                    if(colIndex==4) {
                        if (row.getCell(colIndex) != null && !row.getCell(colIndex).toString().isEmpty()) {
                            String  columnVal = String.valueOf(row.getCell(colIndex));

                            record.setIndicationDOID(columnVal.replace(".0",""));
                        }
                    }
                    if(colIndex==6) {
                        if (row.getCell(colIndex) != null && !row.getCell(colIndex).toString().isEmpty()) {
                            String   columnVal = String.valueOf(row.getCell(colIndex));
                            String[] fdaDesignations=columnVal.split(",");
                            for(String fdaDesignation:fdaDesignations) {
                                ClinicalTrialAdditionalInfo i = new ClinicalTrialAdditionalInfo();
                                i.setNctId(NCTNumber.trim());
                                i.setPropertyName("fda_designation");
                                i.setPropertyValue(fdaDesignation.trim());
                                info.add(i);
                                // record.setFdaDesignation(columnVal);
                            }
                        }
                    }
                    if(colIndex==2) {
                        if (row.getCell(colIndex) != null && !row.getCell(colIndex).toString().isEmpty()) {
                            String    columnVal = String.valueOf(row.getCell(colIndex));
                            String[] relatedIds = columnVal.split(";");
                            for(String id:relatedIds){
                                ClinicalTrialExternalLink link= new ClinicalTrialExternalLink();
                                link.setType("Related NCTID");
                                link.setName(id);
                                link.setLink("https://www.clinicaltrials.gov/study/"+id);
                                link.setNctId(NCTNumber);
                                externalLinks.add(link);

                            }
                        }
                    }

                    if(colIndex==7) {
                        if (row.getCell(colIndex) != null && !row.getCell(colIndex).toString().isEmpty()) {
                            String   columnVal = String.valueOf(row.getCell(colIndex));
                            String compoundName=null;
                            if(columnVal.contains("(")) {
                                String compoundDescription = columnVal.substring(columnVal.indexOf("(") + 1, columnVal.indexOf(")"));
                                record.setCompoundDescription(compoundDescription);
                                 compoundName=columnVal.substring(0,columnVal.indexOf("("));

                            }else{
                                compoundName=columnVal;
                            }
                            String[] names=compoundName.split("/");
                            record.setCompoundName(names[0]);
                            System.out.println("COMPOUN NAME RECORD VAL:"+ record.getCompoundName());
                            if(names.length>1) {


                                for (int i = 1; i <names.length; i++) {
                                    Alias alias=new Alias();
                                    alias.setIdentifier(NCTNumber);
                                    alias.setAlias(names[i]);
                                    alias.setAliasTypeLC(AliasType.getTypeByCode(i).getDescription());
                                    alias.setFieldName("compound");
                                    aliases.add((alias));
                                }
                            }
                        }
                    }

                }

                try {
                    if(record.getNctId()!=null) {
                        System.out.println("ROW " + row.getRowNum() + "\t" + gson.toJson(record));
                        clinicalTrailDAO.updateSomeNewFieldsDataFields(record);

                    }
                }catch (Exception e){
                    System.out.println(record.getNctId());
                    e.printStackTrace();
                }
                try {
                    for(ClinicalTrialExternalLink xLink:externalLinks){
                        if(!clinicalTrailDAO.existsExternalLink(xLink)) {
                            int id=clinicalTrailDAO.getNextKey("clinical_trial_ext_links_seq");
                            xLink.setId(id);
                            clinicalTrailDAO.insertExternalLink(xLink);
                        }
                    }
                }catch (Exception e){
                }
                try {
                    for(Alias alias:aliases){
                        if(!clinicalTrailDAO.existsAlias(alias))
                        clinicalTrailDAO.insertAlias(alias);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
                try {
                    for(ClinicalTrialAdditionalInfo additionalInfo:info){
                        if(!clinicalTrailDAO.existsInfo(additionalInfo))
                            clinicalTrailDAO.insertAdditionalInfo(additionalInfo);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

        }

        fs.close();
    }
    public void parseFileAndMapDB(String file, String sheetName) throws Exception {
        FileInputStream fs=new FileInputStream(new File(file));
        XSSFWorkbook workbook=new XSSFWorkbook(fs);
        XSSFSheet sheet=workbook.getSheet(sheetName);
        if(sheet==null){
           throw new Exception("Sheet is null");
        }
        SimpleDateFormat dateFormat=new SimpleDateFormat("MM/dd/yyy");
        Row headerRow=sheet.getRow(12);
        ObjectMapper mapper=JsonMapper.builder().
                enable( JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER).build();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);

     loop:   for (Row row : sheet) {

            if (row.getRowNum() >12 ) {
                String NCTNumber= String.valueOf(row.getCell(0));
                if( NCTNumber==null || NCTNumber.trim().isEmpty() || NCTNumber.equals("null")){
                    continue loop;
                }

                StringBuilder sb=new StringBuilder();
                    sb.append("{");


                boolean first=true;
                StringBuilder notes=new StringBuilder();
                Iterator<Cell> cellIterator = row.cellIterator();
                while (cellIterator.hasNext() && !NCTNumber.equals("")) {
                    Cell cell = cellIterator.next();
                    int colIndex = cell.getColumnIndex();
                    if(cell.getCellComment()!=null && !cell.getCellComment().toString().isEmpty()){
                            Comment comment=cell.getCellComment();
                           String str=comment.getString().toString();
                           String substr= Arrays.stream(str.substring(str.indexOf("Comment:")+9).split("Reply:")).map(String::trim).collect(Collectors.joining(";"));
                            notes.append(substr.replaceAll("\\s+", " ")).append(" ");
                    }
                    if (headerRow.getCell(colIndex) != null && !headerRow.getCell(colIndex).toString().isEmpty() ) {
                    String columnHeader = String.valueOf(headerRow.getCell(colIndex)).replaceAll(" ", "").replaceAll(":", "");
                        if (first) {
                            sb.append("\"").append(StringUtils.uncapitalize(columnHeader)).append("\":");
                            first = false;
                        } else {
                            sb.append(",").append("\"").append(StringUtils.uncapitalize(columnHeader)).append("\":");

                        }
                    //    System.out.println("COLUMN HEADER:"+columnHeader +"\tCELLTYPE:"+cell.getCellType() +"\tCOLUMN INDEX:"+colIndex);
                        if (cell.getCellType() == CellType.NUMERIC) {
                            if(colIndex==22 || colIndex==23 || colIndex==24)
                            sb.append("\"").append(dateFormat.format(new Date(String.valueOf(cell.getDateCellValue())))).append("\"");
                            else  sb.append("\"").append((int) (cell.getNumericCellValue())).append("\"");

                        } else if (cell.getCellType() == CellType.FORMULA) {
                            if (colIndex == 14) {
                                sb.append("\"").append((int) (cell.getNumericCellValue() * 100)).append("%").append("\"");

                            } else {
                                sb.append("\"").append((int) (cell.getNumericCellValue())).append("\"");

                            }
                        } else {
                            try {
                                sb.append("\"").append(cell.getStringCellValue()).append("\"");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }


                    }
                }
                if(!notes.toString().isEmpty())
                sb.append(",\"notes\":").append("\"").append(notes).append("\"");
                sb.append("}");
                ClinicalTrialRecord rec=mapper.readValue(sb.toString(), ClinicalTrialRecord.class);
                try {
                    clinicalTrailDAO.updateCuratedDataFields(rec);
                }catch (Exception e){
                    e.printStackTrace();
                }
                try {
                    uploadXLinks(sb.toString());
                }catch (Exception e){
                    System.err.println(sb.toString());
                }
            }

        }

        fs.close();
    }
    public void uploadXLinks(String sb) throws Exception {
        JSONObject object=new JSONObject(sb);
        try {
            for (String o : object.get("grants").toString().split(";")) {
                if(o!=null && !o.trim().equals("")) {
                    insertLink( o,  "Grant",  object.get("nCTNumber").toString());
                }
            }
        }catch (Exception e){}
        try {
            for (String o : object.get("protocols").toString().split(";")) {
                if(o!=null  && !o.trim().equals("")) {
                    insertLink( o,  "Protocol",  object.get("nCTNumber").toString());
                }
            }

        }catch (Exception e){}
        try {
            for (String o : object.get("clinicalPublications").toString().split(";")) {
                if(o!=null  && !o.trim().equals("")) {
                    insertLink( o,  "Clinical Publications",  object.get("nCTNumber").toString());
                }
            }
        }catch (Exception e){}
        try {
            for (String o : object.get("preclinicalPublications").toString().split(";")) {
                if(o!=null  && !o.trim().equals("")) {
                    insertLink( o,  "Preclinical Publications",  object.get("nCTNumber").toString());

                }
            }
        }catch (Exception e){}
        try {
            for (String o : object.get("newsandPressReleases").toString().split(";")) {
                if(o!=null  && !o.trim().equals("")) {
                    insertLink( o,  "News and Press Releases",  object.get("nCTNumber").toString());
                }
            }
        }catch (Exception e){}
        try {
            for (String o : object.get("sponsorTrialWebsiteLink").toString().split(";")) {
                if(o!=null  && !o.trim().equals("")) {
                    insertLink( o,  "Sponsor Trial Website Link",  object.get("nCTNumber").toString());
                }
            }

        }catch (Exception e){}

    }
    public void insertLink(String o, String type, String nctNumber) throws Exception {
        ClinicalTrialExternalLink xLink = new ClinicalTrialExternalLink();
        xLink.setName(o.trim());
        xLink.setType(type);
        xLink.setNctId(nctNumber);
        if (o.contains("https") || o.contains("http")) {
            xLink.setLink(o);
        }
        if (o.toLowerCase().contains("pmid")) {
            String pmid = o.substring(o.indexOf(":") + 1).trim();
            xLink.setLink("https://pubmed.ncbi.nlm.nih.gov/" + pmid);
        }

        if(!clinicalTrailDAO.existsExternalLink(xLink)) {
            int id=clinicalTrailDAO.getNextKey("clinical_trial_ext_links_seq");
            xLink.setId(id);
            clinicalTrailDAO.insertExternalLink(xLink);
        }
    }
    public String parseFile(String file, String sheetName) throws Exception {
        FileInputStream fs=new FileInputStream(new File(file));
        XSSFWorkbook workbook=new XSSFWorkbook(fs);
        XSSFSheet sheet=workbook.getSheet(sheetName);
        if(sheet==null){
            return null;
        }
        SimpleDateFormat dateFormat=new SimpleDateFormat("MM/dd/yyy");
        Row headerRow=sheet.getRow(4);
        StringBuilder sb=new StringBuilder();
        ObjectMapper mapper=JsonMapper.builder().
                enable( JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER).build();
        sb.append("{\"studies\":[");
        boolean firstRow=true;
        String sponsor=null;
        loop:   for (Row row : sheet) {

            if (row.getRowNum() >4 ) {
                String NCTNumber= String.valueOf(row.getCell(0));
                if( NCTNumber==null || NCTNumber.trim().isEmpty() || NCTNumber.equals("null")){
                    if(row.getCell(2)!=null)
                        sponsor=row.getCell(2).toString();
                    continue loop;
                }

                if(firstRow) {
                    sb.append("{");
                    firstRow=false;
                }else{
                    sb.append(",{");
                }
                //   sb.append("\"sponsor\":\"").append(sponsor).append("\",");
                sb.append("\"trackerType\":").append("\"").append(sheetName).append("\",");

                boolean first=true;
                StringBuilder notes=new StringBuilder();
                Iterator<Cell> cellIterator = row.cellIterator();
                while (cellIterator.hasNext() && !NCTNumber.equals("")) {
                    Cell cell = cellIterator.next();
                    int colIndex = cell.getColumnIndex();
                    if(cell.getCellComment()!=null && !cell.getCellComment().toString().isEmpty()){
                        Comment comment=cell.getCellComment();
                        String str=comment.getString().toString();
                        String substr= Arrays.stream(str.substring(str.indexOf("Comment:")+9).split("Reply:")).map(String::trim).collect(Collectors.joining(";"));
                        notes.append(substr.replaceAll("\\s+", " ")).append(" ");
                    }
                    if (headerRow.getCell(colIndex) != null && !headerRow.getCell(colIndex).toString().isEmpty() ) {
                        String columnHeader = String.valueOf(headerRow.getCell(colIndex)).replaceAll(" ", "").replaceAll(":", "");
                        if (first) {
                            sb.append("\"").append(StringUtils.uncapitalize(columnHeader)).append("\":");
                            first = false;
                        } else {
                            sb.append(",").append("\"").append(StringUtils.uncapitalize(columnHeader)).append("\":");

                        }
                        //    System.out.println("COLUMN HEADER:"+columnHeader +"\tCELLTYPE:"+cell.getCellType() +"\tCOLUMN INDEX:"+colIndex);
                        if (cell.getCellType() == CellType.NUMERIC) {
                            if(colIndex==22 || colIndex==23 || colIndex==24)
                                sb.append("\"").append(dateFormat.format(new Date(String.valueOf(cell.getDateCellValue())))).append("\"");
                            else  sb.append("\"").append((int) (cell.getNumericCellValue())).append("\"");

                        } else if (cell.getCellType() == CellType.FORMULA) {
                            if (colIndex == 14) {
                                sb.append("\"").append((int) (cell.getNumericCellValue() * 100)).append("%").append("\"");

                            } else {
                                sb.append("\"").append((int) (cell.getNumericCellValue())).append("\"");

                            }
                        } else {
                            try {
                                sb.append("\"").append(cell.getStringCellValue()).append("\"");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }


                    }
                }
                if(!notes.toString().isEmpty())
                    sb.append(",\"notes\":").append("\"").append(notes).append("\"");
                String interventionDescription=getInterventionDescription(NCTNumber);
                if(interventionDescription!=null && !interventionDescription.equals("")) {

                    String intervention=mapper.writeValueAsString(interventionDescription);
                    sb.append(",\"interventionDescription\":").append(intervention);
                }
                sb.append("}");
            }

        }
        sb.append("]}");
        //
        System.out.println(sb.toString());
        fs.close();
        return sb.toString();

    }
    public String getInterventionDescription(String nctId) throws Exception {
      List<ClinicalTrialRecord> records=  clinicalTrailDAO.getClinicalTrailRecordByNctId(nctId);
      if(records!=null && records.size()>0){
          return records.get(0).getInterventionDescription();
      }
        return null;
    }
    public List<String>  parseFileForNCTIds(String file) throws Exception {
        FileInputStream fs=new FileInputStream(new File(file));

        XSSFWorkbook workbook=new XSSFWorkbook(fs);
        XSSFSheet sheet=workbook.getSheet("updated on STAGE");
        if(sheet==null){
            return null;
        }
        List<String> nctIds=new ArrayList<>();
        int i=0;
        for (Row row : sheet) {
            if (row.getRowNum() > 4){
                String NCTNumber = String.valueOf(row.getCell(0));
                nctIds.add(NCTNumber);
            i++;
        }

        }

        fs.close();
        System.out.println("NCTIDS:"+ nctIds.toString());
     return nctIds;
    }
    public void index(String sb) throws IOException {

        JSONObject jsonObject = new JSONObject(sb);
        JSONArray array = (JSONArray) jsonObject.get("studies");
        for (Object object : array) {
        //    indexer.indexDocuments(object);
            IndexRequest request=   new IndexRequest(Index.getNewAlias()).source(object.toString(), XContentType.JSON);
            ESClient.getClient().index(request, RequestOptions.DEFAULT);
        }

        RefreshRequest refreshRequest = new RefreshRequest();
        ESClient.getClient().indices().refresh(refreshRequest, RequestOptions.DEFAULT);
    }
    public void indexFromFile(String file) throws Exception {
        String allData= parseFile(file, "all data");
         index(allData);
    }
    public void parseFileNLoadToDB(String file) throws Exception {
        System.out.println("Processing file to load clinical trial curated fields .." + file);
        parseFileAndMapDB(file, "all data");
    }
    public void indexClinicalTrials() throws Exception {
        Gson gson=new Gson();
        List<ClinicalTrialRecord> trials= clinicalTrailDAO.getAllClinicalTrailRecords();
        System.out.println("CT TRIALS:"+ trials.size());
        ObjectMapper mapper=JsonMapper.builder().
                enable( JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
                //.enable(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION)
                .build();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);


        for(ClinicalTrialRecord trial:trials){
        //   formatRecordValue(trial);
            List<ClinicalTrialExternalLink> externalLinks=  clinicalTrailDAO.getExtLinksByNctId(trial.getNctId());
            if(externalLinks!=null && externalLinks.size()>0)
              trial.setExternalLinks(externalLinks);
            ClinicalTrialIndexObject object=mapper.readValue(gson.toJson(trial), ClinicalTrialIndexObject.class);

          if(trial.getPhase()!=null)
          object.setPhases(Arrays.stream(trial.getPhase().split(",")).map(String::trim).collect(Collectors.toSet()));
            if(trial.getStandardAge()!=null)
          object.setStandardAges(Arrays.stream(trial.getStandardAge().split(",")).map(String::trim).collect(Collectors.toSet()));
            if(trial.getStudyStatus()!=null)
          object.setStatus(Arrays.stream(trial.getStudyStatus().split(",")).map(String::trim).collect(Collectors.toSet()));
            if(trial.getLocation()!=null)
            object.setLocations(Arrays.stream(trial.getLocation().split(",")).map(String::trim).collect(Collectors.toSet()));
            object.setCategory("ClinicalTrial");
          indexClinicalTrailRecord(object);
        }
    }
    public void formatRecordValue(ClinicalTrialRecord record){
        try {
            record.setTargetGeneOrVariant(StringUtils.capitalize(record.getTargetGeneOrVariant()));
        }catch (Exception e){}
        try {
            record.setCompoundName(StringUtils.capitalize(record.getCompoundName()));
        }catch (Exception e){}
        try {
            record.setTherapyType(StringUtils.capitalize(record.getTherapyType()));
        }catch (Exception e){}
        try {
            record.setTherapyRoute(StringUtils.capitalize(record.getTherapyRoute()));
        }catch(Exception e){}
        try {
            record.setMechanismOfAction(StringUtils.capitalize(record.getMechanismOfAction()));
        }catch (Exception e){}
        try {
            record.setRouteOfAdministration(StringUtils.capitalize(record.getRouteOfAdministration()));
        }catch (Exception e){}
        try {
            record.setDrugProductType(StringUtils.capitalize(record.getDrugProductType()));
        }catch (Exception e){}
        try {
            record.setTargetTissueOrCell(StringUtils.capitalize(record.getTargetTissueOrCell()));
        }catch (Exception e){}
        try {
            record.setDeliverySystem(StringUtils.capitalize(record.getDeliverySystem()));
        }catch (Exception e){}
        try {
            if(!record.getDose1().equalsIgnoreCase("none"))
            record.setDose1(StringUtils.capitalize(record.getDose1()));
            else record.setDose1("");
        }catch (Exception e){}
        try {
            if(!record.getDose2().equalsIgnoreCase("none"))
            record.setDose2(StringUtils.capitalize(record.getDose2()));
            else record.setDose2("");
        }catch (Exception e){}
        try {
            if(!record.getDose3().equalsIgnoreCase("none"))
            record.setDose3(StringUtils.capitalize(record.getDose3()));
            else record.setDose3("");
        }catch (Exception e){}
        try {
            if(!record.getDose4().equalsIgnoreCase("none"))
            record.setDose4(StringUtils.capitalize(record.getDose4()));
            else record.setDose4("");
        }catch (Exception e){}
        try {
            if(!record.getDose5().equalsIgnoreCase("none"))
            record.setDose5(StringUtils.capitalize(record.getDose5()));
            else record.setDose5("");
        }catch (Exception e){}

        try {
            if(record.getIsFDARegulated()!=null && !record.getIsFDARegulated().equalsIgnoreCase("null"))
                record.setIsFDARegulated(StringUtils.capitalize(record.getIsFDARegulated()));
            else record.setIsFDARegulated("");
        }catch (Exception e){}
        try {
            if(record.getRecentUpdates()!=null && !record.getRecentUpdates().equalsIgnoreCase("null"))
                record.setRecentUpdates(StringUtils.capitalize(record.getRecentUpdates()));

        }catch (Exception e){}

        if(!record.getStudyStatus().equals("")) record.setStudyStatus(formatFieldVal(record.getStudyStatus()));
        if(!record.getSponsorClass().equals("") && !record.getSponsorClass().equalsIgnoreCase("NIH")) record.setSponsorClass(formatFieldVal(record.getSponsorClass()));
        if(!record.getPhase().equals("")) record.setPhase(formatFieldVal(record.getPhase()));
        if(!record.getStandardAge().equals("")) record.setStandardAge(formatFieldVal(record.getStandardAge()));
    }
    public String formatFieldVal(String fieldVal){
        return  Arrays.stream(fieldVal.split(",")).map(str->StringUtils.capitalize(str.toLowerCase().trim().replaceAll("_", " "))).collect(Collectors.joining(", "));
    }
    public void indexClinicalTrailRecord(ClinicalTrialIndexObject record) throws IOException {

        JSONObject jsonObject = new JSONObject(record);
        IndexRequest request=   new IndexRequest(Index.getNewAlias()).source(jsonObject.toString(), XContentType.JSON);
        ESClient.getClient().index(request, RequestOptions.DEFAULT);
        RefreshRequest refreshRequest = new RefreshRequest();
        ESClient.getClient().indices().refresh(refreshRequest, RequestOptions.DEFAULT);

    }
    public void updateClinicalTrailRecord(ClinicalTrialIndexObject record) throws Exception {

//        JSONObject jsonObject = new JSONObject(record);
//        IndexRequest request=   new IndexRequest(Index.getNewAlias()).source(jsonObject.toString(), XContentType.JSON);
//        ESClient.getClient().index(request, RequestOptions.DEFAULT);
//        RefreshRequest refreshRequest = new RefreshRequest();
//        ESClient.getClient().indices().refresh(refreshRequest, RequestOptions.DEFAULT);
        IndexAdmin indexAdmin=new IndexAdmin();
        indexAdmin.updateIndex(SCGEContext.getESIndexName());
        if(BulkIndexProcessor.bulkProcessor==null){
            BulkIndexProcessor.getInstance();
        }
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        String json = mapper.writeValueAsString(record);
        BulkIndexProcessor.bulkProcessor.add(new IndexRequest(Index.getNewAlias()).source(json, XContentType.JSON));
    }
}
