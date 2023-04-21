package CSCI485ClassProject.models;

import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NonClusteredHashIndexRecord implements IndexRecord{

  private static final IndexType INDEX_TYPE = IndexType.NON_CLUSTERED_HASH_INDEX;

  private final Object attrVal;
  private final List<Object> pkValues;


  public NonClusteredHashIndexRecord(Object attrValue, List<Object> pkValue) {
    this.attrVal = attrValue;
    this.pkValues = pkValue;
  }

  public NonClusteredHashIndexRecord(Tuple keyTuple) {
    this.attrVal = keyTuple.get(0); // the hash val of the attribute
    this.pkValues = new ArrayList<>();
    for (int i = 1; i < keyTuple.size(); i++) {
      this.pkValues.add(keyTuple.get(i));
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
    long attrHashVal = Objects.hashCode(attrVal);
    return new Tuple().add(attrHashVal);
  }
}