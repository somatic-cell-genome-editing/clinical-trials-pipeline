
package edu.mcw.scge.platform.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
    "browseBranches",
    "ancestors",
    "browseLeaves",
    "meshes"
})
@Generated("jsonschema2pojo")
public class ConditionBrowseModule {

    @JsonProperty("browseBranches")
    private List<BrowseBranch> browseBranches = new ArrayList<BrowseBranch>();
    @JsonProperty("ancestors")
    private List<Ancestor> ancestors = new ArrayList<Ancestor>();
    @JsonProperty("browseLeaves")
    private List<Browseleaf> browseLeaves = new ArrayList<Browseleaf>();
    @JsonProperty("meshes")
    private List<Mesh> meshes = new ArrayList<Mesh>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("browseBranches")
    public List<BrowseBranch> getBrowseBranches() {
        return browseBranches;
    }

    @JsonProperty("browseBranches")
    public void setBrowseBranches(List<BrowseBranch> browseBranches) {
        this.browseBranches = browseBranches;
    }

    public ConditionBrowseModule withBrowseBranches(List<BrowseBranch> browseBranches) {
        this.browseBranches = browseBranches;
        return this;
    }

    @JsonProperty("ancestors")
    public List<Ancestor> getAncestors() {
        return ancestors;
    }

    @JsonProperty("ancestors")
    public void setAncestors(List<Ancestor> ancestors) {
        this.ancestors = ancestors;
    }

    public ConditionBrowseModule withAncestors(List<Ancestor> ancestors) {
        this.ancestors = ancestors;
        return this;
    }

    @JsonProperty("browseLeaves")
    public List<Browseleaf> getBrowseLeaves() {
        return browseLeaves;
    }

    @JsonProperty("browseLeaves")
    public void setBrowseLeaves(List<Browseleaf> browseLeaves) {
        this.browseLeaves = browseLeaves;
    }

    public ConditionBrowseModule withBrowseLeaves(List<Browseleaf> browseLeaves) {
        this.browseLeaves = browseLeaves;
        return this;
    }

    @JsonProperty("meshes")
    public List<Mesh> getMeshes() {
        return meshes;
    }

    @JsonProperty("meshes")
    public void setMeshes(List<Mesh> meshes) {
        this.meshes = meshes;
    }

    public ConditionBrowseModule withMeshes(List<Mesh> meshes) {
        this.meshes = meshes;
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

    public ConditionBrowseModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ConditionBrowseModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("browseBranches");
        sb.append('=');
        sb.append(((this.browseBranches == null)?"<null>":this.browseBranches));
        sb.append(',');
        sb.append("ancestors");
        sb.append('=');
        sb.append(((this.ancestors == null)?"<null>":this.ancestors));
        sb.append(',');
        sb.append("browseLeaves");
        sb.append('=');
        sb.append(((this.browseLeaves == null)?"<null>":this.browseLeaves));
        sb.append(',');
        sb.append("meshes");
        sb.append('=');
        sb.append(((this.meshes == null)?"<null>":this.meshes));
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
        result = ((result* 31)+((this.browseBranches == null)? 0 :this.browseBranches.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.ancestors == null)? 0 :this.ancestors.hashCode()));
        result = ((result* 31)+((this.browseLeaves == null)? 0 :this.browseLeaves.hashCode()));
        result = ((result* 31)+((this.meshes == null)? 0 :this.meshes.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ConditionBrowseModule) == false) {
            return false;
        }
        ConditionBrowseModule rhs = ((ConditionBrowseModule) other);
        return ((((((this.browseBranches == rhs.browseBranches)||((this.browseBranches!= null)&&this.browseBranches.equals(rhs.browseBranches)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.ancestors == rhs.ancestors)||((this.ancestors!= null)&&this.ancestors.equals(rhs.ancestors))))&&((this.browseLeaves == rhs.browseLeaves)||((this.browseLeaves!= null)&&this.browseLeaves.equals(rhs.browseLeaves))))&&((this.meshes == rhs.meshes)||((this.meshes!= null)&&this.meshes.equals(rhs.meshes))));
    }

}
