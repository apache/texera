package edu.uci.ics.textdb.api.common;

import java.util.List;

/**
 * Created by chenli on 3/25/16.
 */
public interface ITuple {
    IField getField(int index);
    IField getField(String fieldName);
    List<IField> getFields();
}
