
package edu.mcw.scge.platform.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
    "masking",
    "whoMasked"
})
@Generated("jsonschema2pojo")
public class MaskingInfo {

    @JsonProperty("masking")
    private String masking;
    @JsonProperty("whoMasked")
    private List<String> whoMasked = new ArrayList<String>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("masking")
    public String getMasking() {
        return masking;
    }

    @JsonProperty("masking")
    public void setMasking(String masking) {
        this.masking = masking;
    }

    public MaskingInfo withMasking(String masking) {
        this.masking = masking;
        return this;
    }

    @JsonProperty("whoMasked")
    public List<String> getWhoMasked() {
        return whoMasked;
    }

    @JsonProperty("whoMasked")
    public void setWhoMasked(List<String> whoMasked) {
        this.whoMasked = whoMasked;
    }

    public MaskingInfo withWhoMasked(List<String> whoMasked) {
        this.whoMasked = whoMasked;
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

    public MaskingInfo withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(MaskingInfo.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("masking");
        sb.append('=');
        sb.append(((this.masking == null)?"<null>":this.masking));
        sb.append(',');
        sb.append("whoMasked");
        sb.append('=');
        sb.append(((this.whoMasked == null)?"<null>":this.whoMasked));
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
        result = ((result* 31)+((this.masking == null)? 0 :this.masking.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.whoMasked == null)? 0 :this.whoMasked.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof MaskingInfo) == false) {
            return false;
        }
        MaskingInfo rhs = ((MaskingInfo) other);
        return ((((this.masking == rhs.masking)||((this.masking!= null)&&this.masking.equals(rhs.masking)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.whoMasked == rhs.whoMasked)||((this.whoMasked!= null)&&this.whoMasked.equals(rhs.whoMasked))));
    }

}
