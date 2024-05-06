
package edu.mcw.scge.platform.model;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.processing.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "isFdaRegulatedDrug",
    "isUsExport",
    "oversightHasDmc",
    "isFdaRegulatedDevice"
})
@Generated("jsonschema2pojo")
public class OversightModule {

    @JsonProperty("isFdaRegulatedDrug")
    private Boolean isFdaRegulatedDrug;
    @JsonProperty("isUsExport")
    private Boolean isUsExport;
    @JsonProperty("oversightHasDmc")
    private Boolean oversightHasDmc;
    @JsonProperty("isFdaRegulatedDevice")
    private Boolean isFdaRegulatedDevice;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("isFdaRegulatedDrug")
    public Boolean getIsFdaRegulatedDrug() {
        return isFdaRegulatedDrug;
    }

    @JsonProperty("isFdaRegulatedDrug")
    public void setIsFdaRegulatedDrug(Boolean isFdaRegulatedDrug) {
        this.isFdaRegulatedDrug = isFdaRegulatedDrug;
    }

    public OversightModule withIsFdaRegulatedDrug(Boolean isFdaRegulatedDrug) {
        this.isFdaRegulatedDrug = isFdaRegulatedDrug;
        return this;
    }

    @JsonProperty("isUsExport")
    public Boolean getIsUsExport() {
        return isUsExport;
    }

    @JsonProperty("isUsExport")
    public void setIsUsExport(Boolean isUsExport) {
        this.isUsExport = isUsExport;
    }

    public OversightModule withIsUsExport(Boolean isUsExport) {
        this.isUsExport = isUsExport;
        return this;
    }

    @JsonProperty("oversightHasDmc")
    public Boolean getOversightHasDmc() {
        return oversightHasDmc;
    }

    @JsonProperty("oversightHasDmc")
    public void setOversightHasDmc(Boolean oversightHasDmc) {
        this.oversightHasDmc = oversightHasDmc;
    }

    public OversightModule withOversightHasDmc(Boolean oversightHasDmc) {
        this.oversightHasDmc = oversightHasDmc;
        return this;
    }

    @JsonProperty("isFdaRegulatedDevice")
    public Boolean getIsFdaRegulatedDevice() {
        return isFdaRegulatedDevice;
    }

    @JsonProperty("isFdaRegulatedDevice")
    public void setIsFdaRegulatedDevice(Boolean isFdaRegulatedDevice) {
        this.isFdaRegulatedDevice = isFdaRegulatedDevice;
    }

    public OversightModule withIsFdaRegulatedDevice(Boolean isFdaRegulatedDevice) {
        this.isFdaRegulatedDevice = isFdaRegulatedDevice;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public OversightModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(OversightModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("isFdaRegulatedDrug");
        sb.append('=');
        sb.append(((this.isFdaRegulatedDrug == null)?"<null>":this.isFdaRegulatedDrug));
        sb.append(',');
        sb.append("isUsExport");
        sb.append('=');
        sb.append(((this.isUsExport == null)?"<null>":this.isUsExport));
        sb.append(',');
        sb.append("oversightHasDmc");
        sb.append('=');
        sb.append(((this.oversightHasDmc == null)?"<null>":this.oversightHasDmc));
        sb.append(',');
        sb.append("isFdaRegulatedDevice");
        sb.append('=');
        sb.append(((this.isFdaRegulatedDevice == null)?"<null>":this.isFdaRegulatedDevice));
        sb.append(',');
        sb.append("additionalProperties");
        sb.append('=');
        sb.append(((this.additionalProperties == null)?"<null>":this.additionalProperties));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((this.isFdaRegulatedDrug == null)? 0 :this.isFdaRegulatedDrug.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.oversightHasDmc == null)? 0 :this.oversightHasDmc.hashCode()));
        result = ((result* 31)+((this.isFdaRegulatedDevice == null)? 0 :this.isFdaRegulatedDevice.hashCode()));
        result = ((result* 31)+((this.isUsExport == null)? 0 :this.isUsExport.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof OversightModule) == false) {
            return false;
        }
        OversightModule rhs = ((OversightModule) other);
        return ((((((this.isFdaRegulatedDrug == rhs.isFdaRegulatedDrug)||((this.isFdaRegulatedDrug!= null)&&this.isFdaRegulatedDrug.equals(rhs.isFdaRegulatedDrug)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.oversightHasDmc == rhs.oversightHasDmc)||((this.oversightHasDmc!= null)&&this.oversightHasDmc.equals(rhs.oversightHasDmc))))&&((this.isFdaRegulatedDevice == rhs.isFdaRegulatedDevice)||((this.isFdaRegulatedDevice!= null)&&this.isFdaRegulatedDevice.equals(rhs.isFdaRegulatedDevice))))&&((this.isUsExport == rhs.isUsExport)||((this.isUsExport!= null)&&this.isUsExport.equals(rhs.isUsExport))));
    }

}
