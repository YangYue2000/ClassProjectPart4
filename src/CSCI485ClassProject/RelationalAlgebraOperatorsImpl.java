package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

import java.util.*;

import static org.junit.Assert.assertEquals;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    StatusCode statusCode = predicate.validate();
    if(statusCode!=StatusCode.PREDICATE_OR_EXPRESSION_VALID){
      return null;
    }
    return new SelectIterator(tableName,predicate,mode,isUsingIndex);
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    SelectIterator selectIterator = new SelectIterator(tableName,predicate,Iterator.Mode.READ,isUsingIndex);
    Record rec;
    Set<Record> res = new HashSet<Record>();
    while (true) {
      rec = selectIterator.next();
      if (rec == null) {
        break;
      }
      res.add(rec);
    }
    return res;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    return new ProjectIterator(tableName,attrName,isDuplicateFree);
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    return new ProjectIterator(iterator,attrName,isDuplicateFree);
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    ProjectIterator projectIterator = new ProjectIterator(tableName,attrName,isDuplicateFree);
    Record rec;
    List<Record> res = new ArrayList<>();
    while (true) {
      rec = projectIterator.next();
      if (rec == null) {
        break;
      }
      res.add(rec);
    }
    return res;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    return new JoinIterator(outerIterator,innerIterator,predicate,attrNames);
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {
    RecordsImpl records = new RecordsImpl();
    ArrayList<Object> primaryKeysValues = new ArrayList<>();
    ArrayList<String> attrNames = new ArrayList<>();
    ArrayList<Object> attrValues = new ArrayList<>();
    HashMap<String, Record.Value> kv = record.getMapAttrNameToValue();
    for (Map.Entry<String, Record.Value> entry : kv.entrySet()) {
      String key = entry.getKey();
      Record.Value value = entry.getValue();
      boolean found = false;
      for (String s : primaryKeys) {
        if (s.equals(key)) {
          found = true;
          break;
        }
      }
      if(found){
        primaryKeysValues.add(value.getValue());
      }
      else{
        attrNames.add(key);
        attrValues.add(value.getValue());
      }
    }
    return records.insertRecord(tableName, primaryKeys, primaryKeysValues.toArray(), attrNames.toArray(new String[attrNames.size()]), attrValues.toArray());
  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator) {
    String LeftAttr = assignExp.getLeftAttr();
    String RightAttr = assignExp.getRightAttr();
    long RightVal;
    long coefficient = (int) assignExp.getRightVal();
    Record rec;
    RecordsImpl records = new RecordsImpl();
    Cursor cursor;
    if(dataSourceIterator==null){
      cursor = records.openCursor(tableName, Cursor.Mode.READ);
    }
    else cursor = dataSourceIterator.getCursor();
    boolean first = true;
    while (true) {
      if(dataSourceIterator==null){
        if(first){
          rec = records.getFirst(cursor);
          first = false;
        }
        else{
          rec = records.getNext(cursor);
        }
      }
      else{
        rec = dataSourceIterator.next();
      }
      if (rec == null) {
        break;
      }
      RightVal = (long) rec.getValueForGivenAttrName(RightAttr);
      String[] AttrUpdate = new String[1];
      AttrUpdate[0] = LeftAttr;
      Object[] ValUpdate = new Object[1];
      ValUpdate[0] = coefficient*RightVal;
      StatusCode statusCode = records.updateRecord(cursor,AttrUpdate,ValUpdate);
      if(statusCode!=StatusCode.SUCCESS){
        return statusCode;
      }
    }
    cursor.commit();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode delete(String tableName, Iterator iterator) {
    RecordsImpl records = new RecordsImpl();
    Record rec;
    while(true){
      rec = iterator.next();
      if(rec==null){
        records.commitCursor(iterator.getCursor());
        return StatusCode.SUCCESS;
      }
      StatusCode deleteStatusCode = records.deleteRecord(iterator.getCursor());
      if(deleteStatusCode!=StatusCode.SUCCESS) {
        return deleteStatusCode;
      }
    }
  }
}
