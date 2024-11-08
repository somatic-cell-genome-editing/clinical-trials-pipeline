
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
    "descriptionModule",
    "contactsLocationsModule",
    "eligibilityModule",
    "statusModule",
    "outcomesModule",
    "oversightModule",
    "armsInterventionsModule",
    "sponsorCollaboratorsModule",
    "conditionsModule",
    "identificationModule",
    "designModule"
})
@Generated("jsonschema2pojo")
public class ProtocolSection {

    @JsonProperty("descriptionModule")
    private DescriptionModule descriptionModule;
    @JsonProperty("contactsLocationsModule")
    private ContactsLocationsModule contactsLocationsModule;
    @JsonProperty("eligibilityModule")
    private EligibilityModule eligibilityModule;
    @JsonProperty("statusModule")
    private StatusModule statusModule;
    @JsonProperty("outcomesModule")
    private OutcomesModule outcomesModule;
    @JsonProperty("oversightModule")
    private OversightModule oversightModule;
    @JsonProperty("armsInterventionsModule")
    private ArmsInterventionsModule armsInterventionsModule;
    @JsonProperty("sponsorCollaboratorsModule")
    private SponsorCollaboratorsModule sponsorCollaboratorsModule;
    @JsonProperty("conditionsModule")
    private ConditionsModule conditionsModule;
    @JsonProperty("identificationModule")
    private IdentificationModule identificationModule;
    @JsonProperty("designModule")
    private DesignModule designModule;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("descriptionModule")
    public DescriptionModule getDescriptionModule() {
        return descriptionModule;
    }

    @JsonProperty("descriptionModule")
    public void setDescriptionModule(DescriptionModule descriptionModule) {
        this.descriptionModule = descriptionModule;
    }

    public ProtocolSection withDescriptionModule(DescriptionModule descriptionModule) {
        this.descriptionModule = descriptionModule;
        return this;
    }

    @JsonProperty("contactsLocationsModule")
    public ContactsLocationsModule getContactsLocationsModule() {
        return contactsLocationsModule;
    }

    @JsonProperty("contactsLocationsModule")
    public void setContactsLocationsModule(ContactsLocationsModule contactsLocationsModule) {
        this.contactsLocationsModule = contactsLocationsModule;
    }

    public ProtocolSection withContactsLocationsModule(ContactsLocationsModule contactsLocationsModule) {
        this.contactsLocationsModule = contactsLocationsModule;
        return this;
    }

    @JsonProperty("eligibilityModule")
    public EligibilityModule getEligibilityModule() {
        return eligibilityModule;
    }

    @JsonProperty("eligibilityModule")
    public void setEligibilityModule(EligibilityModule eligibilityModule) {
        this.eligibilityModule = eligibilityModule;
    }

    public ProtocolSection withEligibilityModule(EligibilityModule eligibilityModule) {
        this.eligibilityModule = eligibilityModule;
        return this;
    }

    @JsonProperty("statusModule")
    public StatusModule getStatusModule() {
        return statusModule;
    }

    @JsonProperty("statusModule")
    public void setStatusModule(StatusModule statusModule) {
        this.statusModule = statusModule;
    }

    public ProtocolSection withStatusModule(StatusModule statusModule) {
        this.statusModule = statusModule;
        return this;
    }

    @JsonProperty("outcomesModule")
    public OutcomesModule getOutcomesModule() {
        return outcomesModule;
    }

    @JsonProperty("outcomesModule")
    public void setOutcomesModule(OutcomesModule outcomesModule) {
        this.outcomesModule = outcomesModule;
    }

    public ProtocolSection withOutcomesModule(OutcomesModule outcomesModule) {
        this.outcomesModule = outcomesModule;
        return this;
    }

    @JsonProperty("oversightModule")
    public OversightModule getOversightModule() {
        return oversightModule;
    }

    @JsonProperty("oversightModule")
    public void setOversightModule(OversightModule oversightModule) {
        this.oversightModule = oversightModule;
    }

    public ProtocolSection withOversightModule(OversightModule oversightModule) {
        this.oversightModule = oversightModule;
        return this;
    }

    @JsonProperty("armsInterventionsModule")
    public ArmsInterventionsModule getArmsInterventionsModule() {
        return armsInterventionsModule;
    }

    @JsonProperty("armsInterventionsModule")
    public void setArmsInterventionsModule(ArmsInterventionsModule armsInterventionsModule) {
        this.armsInterventionsModule = armsInterventionsModule;
    }

    public ProtocolSection withArmsInterventionsModule(ArmsInterventionsModule armsInterventionsModule) {
        this.armsInterventionsModule = armsInterventionsModule;
        return this;
    }

    @JsonProperty("sponsorCollaboratorsModule")
    public SponsorCollaboratorsModule getSponsorCollaboratorsModule() {
        return sponsorCollaboratorsModule;
    }

    @JsonProperty("sponsorCollaboratorsModule")
    public void setSponsorCollaboratorsModule(SponsorCollaboratorsModule sponsorCollaboratorsModule) {
        this.sponsorCollaboratorsModule = sponsorCollaboratorsModule;
    }

    public ProtocolSection withSponsorCollaboratorsModule(SponsorCollaboratorsModule sponsorCollaboratorsModule) {
        this.sponsorCollaboratorsModule = sponsorCollaboratorsModule;
        return this;
    }

    @JsonProperty("conditionsModule")
    public ConditionsModule getConditionsModule() {
        return conditionsModule;
    }

    @JsonProperty("conditionsModule")
    public void setConditionsModule(ConditionsModule conditionsModule) {
        this.conditionsModule = conditionsModule;
    }

    public ProtocolSection withConditionsModule(ConditionsModule conditionsModule) {
        this.conditionsModule = conditionsModule;
        return this;
    }

    @JsonProperty("identificationModule")
    public IdentificationModule getIdentificationModule() {
        return identificationModule;
    }

    @JsonProperty("identificationModule")
    public void setIdentificationModule(IdentificationModule identificationModule) {
        this.identificationModule = identificationModule;
    }

    public ProtocolSection withIdentificationModule(IdentificationModule identificationModule) {
        this.identificationModule = identificationModule;
        return this;
    }

    @JsonProperty("designModule")
    public DesignModule getDesignModule() {
        return designModule;
    }

    @JsonProperty("designModule")
    public void setDesignModule(DesignModule designModule) {
        this.designModule = designModule;
    }

    public ProtocolSection withDesignModule(DesignModule designModule) {
        this.designModule = designModule;
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

    public ProtocolSection withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ProtocolSection.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("descriptionModule");
        sb.append('=');
        sb.append(((this.descriptionModule == null)?"<null>":this.descriptionModule));
        sb.append(',');
        sb.append("contactsLocationsModule");
        sb.append('=');
        sb.append(((this.contactsLocationsModule == null)?"<null>":this.contactsLocationsModule));
        sb.append(',');
        sb.append("eligibilityModule");
        sb.append('=');
        sb.append(((this.eligibilityModule == null)?"<null>":this.eligibilityModule));
        sb.append(',');
        sb.append("statusModule");
        sb.append('=');
        sb.append(((this.statusModule == null)?"<null>":this.statusModule));
        sb.append(',');
        sb.append("outcomesModule");
        sb.append('=');
        sb.append(((this.outcomesModule == null)?"<null>":this.outcomesModule));
        sb.append(',');
        sb.append("oversightModule");
        sb.append('=');
        sb.append(((this.oversightModule == null)?"<null>":this.oversightModule));
        sb.append(',');
        sb.append("armsInterventionsModule");
        sb.append('=');
        sb.append(((this.armsInterventionsModule == null)?"<null>":this.armsInterventionsModule));
        sb.append(',');
        sb.append("sponsorCollaboratorsModule");
        sb.append('=');
        sb.append(((this.sponsorCollaboratorsModule == null)?"<null>":this.sponsorCollaboratorsModule));
        sb.append(',');
        sb.append("conditionsModule");
        sb.append('=');
        sb.append(((this.conditionsModule == null)?"<null>":this.conditionsModule));
        sb.append(',');
        sb.append("identificationModule");
        sb.append('=');
        sb.append(((this.identificationModule == null)?"<null>":this.identificationModule));
        sb.append(',');
        sb.append("designModule");
        sb.append('=');
        sb.append(((this.designModule == null)?"<null>":this.designModule));
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
        result = ((result* 31)+((this.outcomesModule == null)? 0 :this.outcomesModule.hashCode()));
        result = ((result* 31)+((this.armsInterventionsModule == null)? 0 :this.armsInterventionsModule.hashCode()));
        result = ((result* 31)+((this.sponsorCollaboratorsModule == null)? 0 :this.sponsorCollaboratorsModule.hashCode()));
        result = ((result* 31)+((this.conditionsModule == null)? 0 :this.conditionsModule.hashCode()));
        result = ((result* 31)+((this.designModule == null)? 0 :this.designModule.hashCode()));
        result = ((result* 31)+((this.descriptionModule == null)? 0 :this.descriptionModule.hashCode()));
        result = ((result* 31)+((this.contactsLocationsModule == null)? 0 :this.contactsLocationsModule.hashCode()));
        result = ((result* 31)+((this.eligibilityModule == null)? 0 :this.eligibilityModule.hashCode()));
        result = ((result* 31)+((this.statusModule == null)? 0 :this.statusModule.hashCode()));
        result = ((result* 31)+((this.oversightModule == null)? 0 :this.oversightModule.hashCode()));
        result = ((result* 31)+((this.identificationModule == null)? 0 :this.identificationModule.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ProtocolSection) == false) {
            return false;
        }
        ProtocolSection rhs = ((ProtocolSection) other);
        return (((((((((((((this.outcomesModule == rhs.outcomesModule)||((this.outcomesModule!= null)&&this.outcomesModule.equals(rhs.outcomesModule)))&&((this.armsInterventionsModule == rhs.armsInterventionsModule)||((this.armsInterventionsModule!= null)&&this.armsInterventionsModule.equals(rhs.armsInterventionsModule))))&&((this.sponsorCollaboratorsModule == rhs.sponsorCollaboratorsModule)||((this.sponsorCollaboratorsModule!= null)&&this.sponsorCollaboratorsModule.equals(rhs.sponsorCollaboratorsModule))))&&((this.conditionsModule == rhs.conditionsModule)||((this.conditionsModule!= null)&&this.conditionsModule.equals(rhs.conditionsModule))))&&((this.designModule == rhs.designModule)||((this.designModule!= null)&&this.designModule.equals(rhs.designModule))))&&((this.descriptionModule == rhs.descriptionModule)||((this.descriptionModule!= null)&&this.descriptionModule.equals(rhs.descriptionModule))))&&((this.contactsLocationsModule == rhs.contactsLocationsModule)||((this.contactsLocationsModule!= null)&&this.contactsLocationsModule.equals(rhs.contactsLocationsModule))))&&((this.eligibilityModule == rhs.eligibilityModule)||((this.eligibilityModule!= null)&&this.eligibilityModule.equals(rhs.eligibilityModule))))&&((this.statusModule == rhs.statusModule)||((this.statusModule!= null)&&this.statusModule.equals(rhs.statusModule))))&&((this.oversightModule == rhs.oversightModule)||((this.oversightModule!= null)&&this.oversightModule.equals(rhs.oversightModule))))&&((this.identificationModule == rhs.identificationModule)||((this.identificationModule!= null)&&this.identificationModule.equals(rhs.identificationModule))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))));
    }

}
