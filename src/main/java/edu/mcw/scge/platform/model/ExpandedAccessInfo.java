
package edu.mcw.scge.platform.model;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "hasExpandedAccess"
})
@Generated("jsonschema2pojo")
public class ExpandedAccessInfo {

    @JsonProperty("hasExpandedAccess")
    private Boolean hasExpandedAccess;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("hasExpandedAccess")
    public Boolean getHasExpandedAccess() {
        return hasExpandedAccess;
    }

    @JsonProperty("hasExpandedAccess")
    public void setHasExpandedAccess(Boolean hasExpandedAccess) {
        this.hasExpandedAccess = hasExpandedAccess;
    }

    public ExpandedAccessInfo withHasExpandedAccess(Boolean hasExpandedAccess) {
        this.hasExpandedAccess = hasExpandedAccess;
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

    public ExpandedAccessInfo withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ExpandedAccessInfo.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("hasExpandedAccess");
        sb.append('=');
        sb.append(((this.hasExpandedAccess == null)?"<null>":this.hasExpandedAccess));
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
        result = ((result* 31)+((this.hasExpandedAccess == null)? 0 :this.hasExpandedAccess.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ExpandedAccessInfo) == false) {
            return false;
        }
        ExpandedAccessInfo rhs = ((ExpandedAccessInfo) other);
        return (((this.hasExpandedAccess == rhs.hasExpandedAccess)||((this.hasExpandedAccess!= null)&&this.hasExpandedAccess.equals(rhs.hasExpandedAccess)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))));
    }

}
