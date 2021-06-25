package edu.uci.ics.texera.dataflow.common;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OperatorGroupConstants {
    
    public static final String SOURCE_GROUP = "Source";
    
    public static final String SEARCH_GROUP = "Search";
    
    public static final String ANALYTICS_GROUP = "Analytics";
    
    public static final String SPLIT_GROUP = "Split";
    
    public static final String JOIN_GROUP = "Join";
    
    public static final String UTILITY_GROUP = "Utilities";
    
    public static final String DATABASE_GROUP = "Database";
    
    public static final String RESULT_GROUP = "View Results";
    
    
    public static class GroupOrder {
        @JsonProperty("groupName")
        public String groupName;
        
        @JsonProperty("groupOrder")
        public Integer groupOrder;
        
        public GroupOrder() { }
        
        public GroupOrder(String groupName, Integer groupOrder) {
            this.groupName = groupName;
            this.groupOrder = groupOrder;
        }
    }
    
    /**
     * The order of the groups to show up in the frontend operator panel.
     * The order numbers are relative.
     */
    public static final List<GroupOrder> OperatorGroupOrderList = new ArrayList<>();
    static {
        OperatorGroupOrderList.add(new GroupOrder(SOURCE_GROUP, 0));
        OperatorGroupOrderList.add(new GroupOrder(SEARCH_GROUP, 1));
        OperatorGroupOrderList.add(new GroupOrder(ANALYTICS_GROUP, 2));
        OperatorGroupOrderList.add(new GroupOrder(SPLIT_GROUP, 3));
        OperatorGroupOrderList.add(new GroupOrder(JOIN_GROUP, 4));
        OperatorGroupOrderList.add(new GroupOrder(UTILITY_GROUP, 5));
        OperatorGroupOrderList.add(new GroupOrder(DATABASE_GROUP, 6));
        OperatorGroupOrderList.add(new GroupOrder(RESULT_GROUP, 7));
    }
    

}
