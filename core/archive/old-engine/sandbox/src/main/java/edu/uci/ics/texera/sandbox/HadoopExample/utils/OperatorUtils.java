package edu.uci.ics.texera.sandbox.HadoopExample.utils;

import edu.uci.ics.texera.sandbox.HadoopExample.mr.QueryPlan;
import edu.uci.ics.texera.sandbox.HadoopExample.operator.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class OperatorUtils {
    public static final String SCAN = "ScanSource";
    public static final String KEYWORD = "KeywordMatcher";
    public static final String COUNT = "Count";
    public static final String COUTSHUFFLE = "CountShuffle";
    public static final String FILTER = "Filter";

    public static Operator buildQueryOperator(String queryPlan){
        JSONArray ops = new JSONArray(queryPlan);
        //JSONArray ops = json.getJSONArray("Operators");
        Operator pre=null, cur = null;

        for(int i=0; i<ops.length(); i++){
            JSONObject op = ops.getJSONObject(i);

            switch (op.getString("operatorType")){
                case SCAN:
                    cur = new ScanOperator();
                    break;
                case KEYWORD:
                    cur = new KeywordOperator(op.getString("attributes"), op.getString("query"));
                    break;
                case COUTSHUFFLE:
                    cur = new CountShuffleOperator(op.getString("attributes"));
                    break;
                case FILTER:
                    cur = new FilterOperator(op.getString("query"), op.getString("comp"));
                    break;
                case COUNT:
                    cur = new CountOperator();
                    break;
            }
            if(pre!=null){
                //System.out.println();
                ArrayList<Operator> pa = new ArrayList<>();
                pa.add(pre);
                cur.setParentOperators(pa);
            }
            pre = cur;
        }
        return cur;
    }


    public static List<QueryPlan> buildQueryPlan(String queryPlan){
        List<QueryPlan> queryPlans = new ArrayList<>();
        JSONObject json = new JSONObject(queryPlan);
        JSONArray ops = json.getJSONArray("operators");
        int i = 0;
        while(i<ops.length()){
            QueryPlan qp = new QueryPlan();
            JSONArray map = new JSONArray();
            JSONArray reduce = new JSONArray();
            while(i<ops.length()){
                JSONObject op = ops.getJSONObject(i);
                String opType = op.getString("operatorType");
                i++;
                if(opType.equals(SCAN) || opType.equals(KEYWORD)||opType.equals(FILTER)){
                    map.put(op);
                }else if(opType.equals(COUNT)){
                    JSONObject newop = new JSONObject(op.toString());
                    newop.put("operatorType", COUTSHUFFLE);
                    map.put(newop);
                    reduce.put(op);
                    break;
                }
            }
            if(map.length()>0) {
                qp.mapPlan = map.toString();
            }else{
                qp.mapPlan = "";
            }
            if(reduce.length()>0){
                qp.reducePlan = reduce.toString();
            }else {
                qp.reducePlan = "";
            }
            queryPlans.add(qp);
        }

        return queryPlans;
    }
}
