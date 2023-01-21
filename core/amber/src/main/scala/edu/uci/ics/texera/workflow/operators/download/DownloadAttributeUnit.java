package edu.uci.ics.texera.workflow.operators.download;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle;
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName;
import org.jooq.tools.StringUtils;

import java.util.Objects;

public class DownloadAttributeUnit {
    @JsonProperty(required = true)
    @JsonSchemaTitle("URL Attribute")
    @AutofillAttributeName
    private String urlAtttribute;

    @JsonProperty
    @JsonSchemaTitle("Result Attribute")
    @JsonPropertyDescription("Downloaded file path stored in this new attribute")
    private String resultAttribute;

    DownloadAttributeUnit(String urlAtttribute, String resultAttribute) {
        this.urlAtttribute = urlAtttribute;
        this.resultAttribute = resultAttribute;
    }

    String getUrlAttribute(){
        return urlAtttribute;
    }


    String getResultAttribute(){
        if(StringUtils.isBlank(resultAttribute)){
            return urlAtttribute + " Result";
        }
        return resultAttribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DownloadAttributeUnit that = (DownloadAttributeUnit) o;
        return Objects.equals(urlAtttribute, that.urlAtttribute) && Objects.equals(resultAttribute, that.resultAttribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(urlAtttribute, resultAttribute);
    }
}
