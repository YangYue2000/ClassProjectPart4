package CSCI485ClassProject.models;

import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class NonClusteredBPTreeIndexRecord implements IndexRecord{
  private static IndexType INDEX_TYPE = IndexType.NON_CLUSTERED_HASH_INDEX;

  private Object attrVal;
  private List<Object> pkValues;

  public NonClusteredBPTreeIndexRecord(Object attrVal, List<Object> pkValues) {
    this.attrVal = attrVal;
    this.pkValues = pkValues;
  }

  public NonClusteredBPTreeIndexRecord(Tuple keyTuple) {
    this.attrVal = keyTuple.get(0);
    this.pkValues = new ArrayList<>();
    for (int i = 1; i < keyTuple.size(); i++) {
      pkValues.add(keyTuple.get(i));
    }
  }

  @Override
  public IndexType getIndexType() {
    return INDEX_TYPE;
  }

  @Override
  public Tuple getKeyTuple() {
    Tuple keyTuple = getKeyPrefixTuple(attrVal);
    for (Object pkVal : pkValues) {
      keyTuple = keyTuple.addObject(pkVal);
    }
    return keyTuple;
  }

  @Override
  public Tuple getValueTuple() {
    Tuple valueTuple = new Tuple();
    return valueTuple;
  }

  @Override
  public List<Object> getPrimaryKeys() {
    return pkValues;
  }

  static public Tuple getKeyPrefixTuple(Object attrVal) {
    return new Tuple().addObject(attrVal);
  }
}
