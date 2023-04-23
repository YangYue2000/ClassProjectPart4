package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;

import java.util.*;

public class JoinIterator extends Iterator{
    private final SelectIterator outerIterator;
    private final SelectIterator OriginalInnerIterator;
    private final Object coefficient;
    private final AttributeType type;
    private final ComparisonOperator operator;
    private final String outerAttr;
    private final String innerAttr;
    JoinIterator(Iterator outerIterator, Iterator innerIterator,
                 ComparisonPredicate predicate, Set<String> attrNames){
        this.outerIterator = (SelectIterator) outerIterator;
        this.OriginalInnerIterator = (SelectIterator) innerIterator;
        coefficient = predicate.getRightHandSideValue();
        type = predicate.getLeftHandSideAttrType();
        operator = predicate.getOperator();
        outerAttr = predicate.getLeftHandSideAttrName();
        innerAttr = predicate.getRightHandSideAttrName();
    }
    @Override
    public Record next() {
        Record outerRecord;
        Record innerRecord;
        while (true) {
           outerRecord = outerIterator.next();
            if (outerRecord == null) {
                return null;
            }
            Object outerVal = outerRecord.getValueForGivenAttrName(outerAttr);
            SelectIterator innerIterator = new SelectIterator(OriginalInnerIterator.getTableNameOfRecord(),OriginalInnerIterator.getPredicate(),OriginalInnerIterator.getmode(),OriginalInnerIterator.getIsUsingIndex());
            while(true){
                innerRecord = innerIterator.next();
                if(innerRecord == null){
                    break;
                }
                Object innerVal = innerRecord.getValueForGivenAttrName(innerAttr);
                if (type == AttributeType.INT && ComparisonUtils.compareTwoINT(outerVal, innerVal, coefficient, operator)) {
                    Set<String> outerSet = new HashSet<>();
                    HashMap<String,Record.Value> outerMap= outerRecord.getMapAttrNameToValue();
                    for (Map.Entry<String, Record.Value> entry : outerMap.entrySet()) {
                        outerSet.add(entry.getKey());
                    }
                    Set<String> innerSet = new HashSet<>();
                    HashMap<String,Record.Value> innerMap= innerRecord.getMapAttrNameToValue();
                    for (Map.Entry<String, Record.Value> entry : innerMap.entrySet()) {
                        innerSet.add(entry.getKey());
                    }
                    // find the duplicates between outerSet and innerSet
                    Set<String> duplicates = new HashSet<>(outerSet);
                    duplicates.retainAll(innerSet);
                    Record record = new Record();
                    for (Map.Entry<String, Record.Value> entry : outerMap.entrySet()) {
                        String attrName = "";
                        if(duplicates.contains(entry.getKey())){
                            attrName += outerIterator.getTableNameOfRecord()+".";
                        }
                        attrName += entry.getKey();
                        record.setAttrNameAndValue(attrName,entry.getValue().getValue());
                    }
                    for (Map.Entry<String, Record.Value> entry : innerMap.entrySet()) {
                        String attrName = "";
                        if(duplicates.contains(entry.getKey())){
                            attrName += innerIterator.getTableNameOfRecord()+".";
                        }
                        attrName += entry.getKey();
                        record.setAttrNameAndValue(attrName,entry.getValue().getValue());
                    }
                    return record;
                }

            }
        }
    }

    @Override
    public void commit() {
        outerIterator.commit();
        OriginalInnerIterator.commit();
    }

    @Override
    public void abort() {
        outerIterator.abort();
        OriginalInnerIterator.abort();
    }
}
