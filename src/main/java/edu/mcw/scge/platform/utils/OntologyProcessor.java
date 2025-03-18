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
        String baseURI = "https://dev.rgd.mcw.edu/rgdws/ontology/parentTerms/";
        RestTemplate restTemplate=new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();
        for (ClinicalTrialRecord record : records) {
            String ontId=record.getIndicationDOID();
            if (ontId != null) {
                String fetchURI = baseURI +"DOID:"+ontId;
                try {
                    String response = restTemplate.getForObject(fetchURI, String.class);
                    if (response != null && !response.isEmpty() && response.length() > 2) {
                        Map<String, String> idMap = mapper.readValue(response, Map.class);
//                        System.out.println("ID MAP:" + gson.toJson(idMap));
                        for(String key:idMap.keySet()){
                            uploadInfoObject(key, record.getNctId());
                            uploadInfoObject(idMap.get(key).toString(), record.getNctId());
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
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
