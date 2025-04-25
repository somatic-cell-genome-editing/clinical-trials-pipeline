package edu.mcw.scge.platform.utils;



import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import edu.mcw.scge.dao.implementation.ClinicalTrailDAO;
import edu.mcw.scge.datamodel.ClinicalTrialAdditionalInfo;
import edu.mcw.scge.datamodel.ClinicalTrialRecord;
import org.springframework.web.client.RestTemplate;
;
import java.util.List;
import java.util.Map;

public class OntologyProcessor {
//    Gson gson=new Gson();
    ClinicalTrailDAO clinicalTrailDAO=new ClinicalTrailDAO();
    public void uploadParentTerms() throws Exception {
        List<ClinicalTrialRecord> records = clinicalTrailDAO.getAllClinicalTrailRecords();
        String baseURI = "https://rest.rgd.mcw.edu/rgdws/ontology/termAndParentTermWithSynonyms/";
        RestTemplate restTemplate=new RestTemplate();
        restTemplate.setErrorHandler(new ApiResponseErrorHandler());
        ObjectMapper mapper = new ObjectMapper();
        for (ClinicalTrialRecord record : records) {
            String ontId=record.getIndicationDOID();
            if (ontId != null && !ontId.equals("")) {
                int id=Integer.parseInt(ontId);
                if(id>0) {
                    String fetchURI = baseURI + "DOID:" + ontId;
                    try {
                        List response = restTemplate.getForObject(fetchURI, List.class);
//                    System.out.println("res"+ response);
                        if (response != null && response.size() > 0) {
                            for (Object synonym : response) {
                                uploadInfoObject(synonym.toString(), record.getNctId());
                            }
                        }

//                    if (response != null && response.length() > 2) {
//                       List synonyms = mapper.readValue(response, List.class);
////                        System.out.println("ID MAP:" + gson.toJson(idMap));
//                        for(Object synonym:synonyms){
//                            uploadInfoObject(synonym.toString(), record.getNctId());
//
//                        }
//                    }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
    public void uploadInfoObject(String value, String nctId) throws Exception {
        ClinicalTrialAdditionalInfo info=new ClinicalTrialAdditionalInfo();
        info.setPropertyName("indication_ont_parent_term");
        info.setPropertyValue(value);
        info.setNctId(nctId);
        if(!clinicalTrailDAO.existsInfo(info))
            clinicalTrailDAO.insertAdditionalInfo(info);
    }
}
