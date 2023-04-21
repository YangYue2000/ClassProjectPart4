package CSCI485ClassProject.models;

import com.apple.foundationdb.tuple.Tuple;

import java.util.List;

public interface IndexRecord {

  public IndexType getIndexType();

  public Tuple getKeyTuple();

  public Tuple getValueTuple();

  public List<Object> getPrimaryKeys();
}
