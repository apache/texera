package edu.uci.ics.textdb.dataflow.nameentityextractor;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringField;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jiaoyang on 4/25/16.
 */
public class NameEntity implements ITuple{

    private String WORD = "word";
    private String ENTITY_TYPE = "entity type";
    private Attribute WORD_ATTR = new Attribute(WORD, FieldType.STRING);
    private Attribute ENTITY_ATTR = new Attribute(ENTITY_TYPE, FieldType.STRING);
    private List<Attribute> NESchema = Arrays.asList(WORD_ATTR, ENTITY_ATTR);
    private ITuple NEtuple;
    private IField[] NEfields;


//    public enum ENTITY{ Location, Person, Organization ,Misc, Money, Percent, Date };

    public NameEntity(String word, String type){

        IField[] NEfields = {new StringField(word), new StringField(type)};
        NEtuple = new DataTuple(NESchema, NEfields);
    }


    @Override
    public IField getField(int index){
        return NEfields[index];
    }


    @Override
    public IField getField(String fieldName){
        for (IField ifield : NEfields){
            if (ifield.getValue() == fieldName){
                return ifield;
            }
        }
        return null;
    }


    @Override
    public List<IField> getFields(){
        return Arrays.asList(NEfields);
    }


    @Override
    public List<Attribute> getSchema(){
        return NESchema;
    }
}
