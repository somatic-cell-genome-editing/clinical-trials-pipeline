package edu.mcw.scge.platform.utils;



import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import edu.mcw.scge.dao.implementation.ClinicalTrailDAO;
import edu.mcw.scge.datamodel.ClinicalTrialAdditionalInfo;
import edu.mcw.scge.datamodel.ClinicalTrialRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.client.RestTemplate;
;
import java.util.List;
import java.util.Map;

public class OntologyProcessor {
//    Gson gson=new Gson();
    ClinicalTrailDAO clinicalTrailDAO=new ClinicalTrailDAO();
    public Logger logger = LogManager.getLogger("ontology");
    public void uploadParentTerms() throws Exception {
        List<ClinicalTrialRecord> records = clinicalTrailDAO.getAllClinicalTrailRecords();
        String baseURI = "https://pipelines.rgd.mcw.edu/rgdws/ontology/termAndParentTermWithSynonyms/";
        RestTemplate restTemplate=new RestTemplate();
        restTemplate.setErrorHandler(new ApiResponseErrorHandler());
        for (ClinicalTrialRecord record : records) {
            String ontId=record.getIndicationDOID();
            if (ontId != null && !ontId.equals("")) {
                for(String id:ontId.split(",")) {
                    String fetchURI = baseURI + id;
                    try {
                        List response = restTemplate.getForObject(fetchURI, List.class);
                        if (response != null && response.size() > 0) {
                            for (Object synonym : response) {
                                uploadInfoObject(synonym.toString(), record.getNctId());
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.info("ERROR ONT ID:"+id);
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
