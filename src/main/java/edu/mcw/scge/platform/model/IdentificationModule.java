
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
    "orgStudyIdInfo",
    "nctId",
    "organization",
    "briefTitle",
    "officialTitle"
})
@Generated("jsonschema2pojo")
public class IdentificationModule {

    @JsonProperty("orgStudyIdInfo")
    private OrgStudyIdInfo orgStudyIdInfo;
    @JsonProperty("nctId")
    private String nctId;
    @JsonProperty("organization")
    private Organization organization;
    @JsonProperty("briefTitle")
    private String briefTitle;
    @JsonProperty("officialTitle")
    private String officialTitle;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("orgStudyIdInfo")
    public OrgStudyIdInfo getOrgStudyIdInfo() {
        return orgStudyIdInfo;
    }

    @JsonProperty("orgStudyIdInfo")
    public void setOrgStudyIdInfo(OrgStudyIdInfo orgStudyIdInfo) {
        this.orgStudyIdInfo = orgStudyIdInfo;
    }

    public IdentificationModule withOrgStudyIdInfo(OrgStudyIdInfo orgStudyIdInfo) {
        this.orgStudyIdInfo = orgStudyIdInfo;
        return this;
    }

    @JsonProperty("nctId")
    public String getNctId() {
        return nctId;
    }

    @JsonProperty("nctId")
    public void setNctId(String nctId) {
        this.nctId = nctId;
    }

    public IdentificationModule withNctId(String nctId) {
        this.nctId = nctId;
        return this;
    }

    @JsonProperty("organization")
    public Organization getOrganization() {
        return organization;
    }

    @JsonProperty("organization")
    public void setOrganization(Organization organization) {
        this.organization = organization;
    }

    public IdentificationModule withOrganization(Organization organization) {
        this.organization = organization;
        return this;
    }

    @JsonProperty("briefTitle")
    public String getBriefTitle() {
        return briefTitle;
    }

    @JsonProperty("briefTitle")
    public void setBriefTitle(String briefTitle) {
        this.briefTitle = briefTitle;
    }

    public IdentificationModule withBriefTitle(String briefTitle) {
        this.briefTitle = briefTitle;
        return this;
    }

    @JsonProperty("officialTitle")
    public String getOfficialTitle() {
        return officialTitle;
    }

    @JsonProperty("officialTitle")
    public void setOfficialTitle(String officialTitle) {
        this.officialTitle = officialTitle;
    }

    public IdentificationModule withOfficialTitle(String officialTitle) {
        this.officialTitle = officialTitle;
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

    public IdentificationModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(IdentificationModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("orgStudyIdInfo");
        sb.append('=');
        sb.append(((this.orgStudyIdInfo == null)?"<null>":this.orgStudyIdInfo));
        sb.append(',');
        sb.append("nctId");
        sb.append('=');
        sb.append(((this.nctId == null)?"<null>":this.nctId));
        sb.append(',');
        sb.append("organization");
        sb.append('=');
        sb.append(((this.organization == null)?"<null>":this.organization));
        sb.append(',');
        sb.append("briefTitle");
        sb.append('=');
        sb.append(((this.briefTitle == null)?"<null>":this.briefTitle));
        sb.append(',');
        sb.append("officialTitle");
        sb.append('=');
        sb.append(((this.officialTitle == null)?"<null>":this.officialTitle));
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
        result = ((result* 31)+((this.orgStudyIdInfo == null)? 0 :this.orgStudyIdInfo.hashCode()));
        result = ((result* 31)+((this.nctId == null)? 0 :this.nctId.hashCode()));
        result = ((result* 31)+((this.organization == null)? 0 :this.organization.hashCode()));
        result = ((result* 31)+((this.briefTitle == null)? 0 :this.briefTitle.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.officialTitle == null)? 0 :this.officialTitle.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof IdentificationModule) == false) {
            return false;
        }
        IdentificationModule rhs = ((IdentificationModule) other);
        return (((((((this.orgStudyIdInfo == rhs.orgStudyIdInfo)||((this.orgStudyIdInfo!= null)&&this.orgStudyIdInfo.equals(rhs.orgStudyIdInfo)))&&((this.nctId == rhs.nctId)||((this.nctId!= null)&&this.nctId.equals(rhs.nctId))))&&((this.organization == rhs.organization)||((this.organization!= null)&&this.organization.equals(rhs.organization))))&&((this.briefTitle == rhs.briefTitle)||((this.briefTitle!= null)&&this.briefTitle.equals(rhs.briefTitle))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.officialTitle == rhs.officialTitle)||((this.officialTitle!= null)&&this.officialTitle.equals(rhs.officialTitle))));
    }

}
