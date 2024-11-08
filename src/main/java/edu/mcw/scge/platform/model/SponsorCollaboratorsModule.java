
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
    "leadSponsor",
    "responsibleParty"
})
@Generated("jsonschema2pojo")
public class SponsorCollaboratorsModule {

    @JsonProperty("leadSponsor")
    private LeadSponsor leadSponsor;
    @JsonProperty("responsibleParty")
    private ResponsibleParty responsibleParty;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("leadSponsor")
    public LeadSponsor getLeadSponsor() {
        return leadSponsor;
    }

    @JsonProperty("leadSponsor")
    public void setLeadSponsor(LeadSponsor leadSponsor) {
        this.leadSponsor = leadSponsor;
    }

    public SponsorCollaboratorsModule withLeadSponsor(LeadSponsor leadSponsor) {
        this.leadSponsor = leadSponsor;
        return this;
    }

    @JsonProperty("responsibleParty")
    public ResponsibleParty getResponsibleParty() {
        return responsibleParty;
    }

    @JsonProperty("responsibleParty")
    public void setResponsibleParty(ResponsibleParty responsibleParty) {
        this.responsibleParty = responsibleParty;
    }

    public SponsorCollaboratorsModule withResponsibleParty(ResponsibleParty responsibleParty) {
        this.responsibleParty = responsibleParty;
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

    public SponsorCollaboratorsModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(SponsorCollaboratorsModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("leadSponsor");
        sb.append('=');
        sb.append(((this.leadSponsor == null)?"<null>":this.leadSponsor));
        sb.append(',');
        sb.append("responsibleParty");
        sb.append('=');
        sb.append(((this.responsibleParty == null)?"<null>":this.responsibleParty));
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
        result = ((result* 31)+((this.leadSponsor == null)? 0 :this.leadSponsor.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.responsibleParty == null)? 0 :this.responsibleParty.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof SponsorCollaboratorsModule) == false) {
            return false;
        }
        SponsorCollaboratorsModule rhs = ((SponsorCollaboratorsModule) other);
        return ((((this.leadSponsor == rhs.leadSponsor)||((this.leadSponsor!= null)&&this.leadSponsor.equals(rhs.leadSponsor)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.responsibleParty == rhs.responsibleParty)||((this.responsibleParty!= null)&&this.responsibleParty.equals(rhs.responsibleParty))));
    }

}
