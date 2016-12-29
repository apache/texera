package edu.uci.ics.textdb.textql.planbuilder.beans;

import java.util.HashMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Bean Container for properties of a ScanBasedSourceOperator
 * 
 * @author Flavio Bayer
 *
 */
public class ScanSourceBean extends OperatorBean {
    /**
     * The name of the view used as source of tuples
     */
    private String dataSource;
    
    public ScanSourceBean() {
        
    }
    
    public ScanSourceBean(String operatorID, String operatorType, String attributes, String limit, String offset, String dataSource) {
        super(operatorID, "ScanSource", attributes, limit, offset);
        this.dataSource = dataSource;
    }
    
    /**
     * Return the name of the view used as source of tuples
     * @return The name of the view used as source of tuples
     */
    public String getDataSource() {
        return dataSource;
    }
    
    /**
     * Set the name of the view used as source of tuples
     * @param dataSource The name of the view used as source of tuples
     */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }
    
    /**
     * Build a map containing the properties of the this operator
     * @return The generated map containing the properties of this operator
     */
    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(this.getDataSource() == null || operatorProperties == null)
            return null;
        operatorProperties.put(OperatorBuilderUtils.DATA_DIRECTORY, this.getDataSource());
        return operatorProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof ScanSourceBean)) return false;
        ScanSourceBean scanSourceBean = (ScanSourceBean) other;
        return new EqualsBuilder()
                    .appendSuper(super.equals(scanSourceBean))
                    .append(dataSource, scanSourceBean.getDataSource())
                    .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                    .append(super.hashCode())
                    .append(dataSource)
                    .toHashCode();
    }
}