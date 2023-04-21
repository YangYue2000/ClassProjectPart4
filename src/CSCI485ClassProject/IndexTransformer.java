package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.IndexRecord;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.NonClusteredBPTreeIndexRecord;
import CSCI485ClassProject.models.NonClusteredHashIndexRecord;
import CSCI485ClassProject.utils.IndexesUtils;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class IndexTransformer {

  private List<String> indexStore;

  public IndexTransformer(String tableName, String attrName, IndexType indexType) {
    indexStore = new ArrayList<>();
    indexStore = IndexesUtils.getAttributeIndexDirPath(tableName, attrName);
    indexStore.add(indexType.name());
  }



  public List<String> getIndexStorePath() {
    return indexStore;
  }

  public FDBKVPair convertToIndexKVPair(IndexType idxType, Object attrVal, List<Object> pkValues) {
    FDBKVPair res = null;
    if (idxType == IndexType.NON_CLUSTERED_HASH_INDEX) {
      NonClusteredHashIndexRecord ncHashIndexRecord = new NonClusteredHashIndexRecord(attrVal, pkValues);
      res = new FDBKVPair(indexStore, ncHashIndexRecord.getKeyTuple(), ncHashIndexRecord.getValueTuple());
    } else if (idxType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
      NonClusteredBPTreeIndexRecord ncBPTreeIndexRecord = new NonClusteredBPTreeIndexRecord(attrVal, pkValues);
      res = new FDBKVPair(indexStore, ncBPTreeIndexRecord.getKeyTuple(), ncBPTreeIndexRecord.getValueTuple());
    }
    return res;
  }

  public IndexRecord convertBackToIndexRecord(IndexType indexType, FDBKVPair kv) {
    if (kv == null) {
      return null;
    }
    Tuple keyTuple = kv.getKey();
    if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX)
      return new NonClusteredHashIndexRecord(keyTuple);
    else if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX)
      return new NonClusteredBPTreeIndexRecord(keyTuple);
    return null;
  }
}
