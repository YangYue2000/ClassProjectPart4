package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.utils.ComparisonUtils;
import CSCI485ClassProject.utils.IndexesUtils;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static CSCI485ClassProject.RecordsTransformer.getPrimaryKeyValTuple;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE,
  }

  // used by predicate
  private boolean isPredicateEnabled = false;
  private String predicateAttributeName;
  private CSCI485ClassProject.models.Record.Value predicateAttributeValue;
  private ComparisonOperator predicateOperator;
  //predicate for iterator
  private ComparisonPredicate comparisonPredicate;

  // Table Schema Info
  private String tableName;
  private TableMetadata tableMetadata;

  private RecordsTransformer recordsTransformer;

  private boolean isInitialized = false;

  private boolean isInitializedToLast = false;

  private final Mode mode;

  private AsyncIterator<KeyValue> iterator = null;

  private CSCI485ClassProject.models.Record currentRecord = null;

  private Transaction tx;

  private DirectorySubspace dataRecordSubspace;

  // used by cursor with predicate and using index
  private boolean isUsingIndex = false;
  private String indexedAttrName;
  private DirectorySubspace indexRecordSubspace;
  private AsyncIterator<KeyValue> indexIterator = null;
  private Tuple indexPrefixQueryTuple;
  private IndexType indexType;
  private boolean isIndexUsable = false;


  // used by cursor updating and deleting
  private Map<String, DirectorySubspace> attrNameToIndexSubspace;
  private Map<String, IndexType> attrNameToIndexType;

  private boolean isMoved = false;
  private FDBKVPair currentKVPair = null;

  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.tx = tx;
  }

  // enable the use of index for READ mode
  public Cursor(String tableName, TableMetadata tableMetadata, String indexedAttrName, ComparisonOperator predicateOperator, CSCI485ClassProject.models.Record.Value predicateVal, IndexType indexType, Transaction tx) {
    this.mode = Mode.READ;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.isUsingIndex = true;
    this.indexType = indexType;
    this.indexedAttrName = indexedAttrName;
    enablePredicate(indexedAttrName, predicateVal, predicateOperator);
    this.tx = tx;
  }

  public void setTx(Transaction tx) {
    this.tx = tx;
  }

  public Transaction getTx() {
    return tx;
  }

  public void abort() {
    if (iterator != null) {
      iterator.cancel();
    }
    if (indexIterator != null) {
      indexIterator.cancel();
    }
    if (tx != null) {
      FDBHelper.abortTransaction(tx);
    }
    tx = null;
  }

  public void commit() {
    if (iterator != null) {
      iterator.cancel();
    }
    if (indexIterator != null) {
      indexIterator.cancel();
    }
    if (tx != null) {
      FDBHelper.commitTransaction(tx);
    }
    tx = null;
  }

  public final Mode getMode() {
    return mode;
  }

  public boolean isInitialized() {
    return isInitialized;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public void setTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public void enablePredicate(String attrName, CSCI485ClassProject.models.Record.Value value, ComparisonOperator operator) {
    this.predicateAttributeName = attrName;
    this.predicateAttributeValue = value;
    this.predicateOperator = operator;
    this.isPredicateEnabled = true;
  }

  //for iterator
  public void enablePredicate(ComparisonPredicate comparisonPredicate){
    this.comparisonPredicate = comparisonPredicate;
    this.isPredicateEnabled = true;
  }

  private void enableIndexMaintenance() {
    this.attrNameToIndexSubspace = IndexesUtils.openIndexSubspacesOfTable(tx, tableName, tableMetadata);
    this.attrNameToIndexType = new HashMap<>();
    for (Map.Entry<String, DirectorySubspace> entry : attrNameToIndexSubspace.entrySet()) {
      String attrName = entry.getKey();
      DirectorySubspace dir = entry.getValue();
      List<String> dirPath = dir.getPath();

      IndexType idxType = IndexType.valueOf(dirPath.get(dirPath.size() - 1));
      attrNameToIndexType.put(attrName, idxType);
    }
  }

  private void enableQueryUsingIndex() {
    if (!isUsingIndex) {
      return;
    }
    IndexTransformer indexTransformer = new IndexTransformer(tableName, indexedAttrName, indexType);
    indexRecordSubspace = FDBHelper.openSubspace(tx, indexTransformer.getIndexStorePath());

    // construct the prefix query tuple
    if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
      indexPrefixQueryTuple = NonClusteredBPTreeIndexRecord.getKeyPrefixTuple(predicateAttributeValue.getValue());
    } else if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
      indexPrefixQueryTuple = NonClusteredHashIndexRecord.getKeyPrefixTuple(predicateAttributeValue.getValue());
    }

    if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
      if (predicateOperator == ComparisonOperator.EQUAL_TO) {
        isIndexUsable = true;
        indexIterator = FDBHelper.getKVPairIterableWithPrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, isInitializedToLast).iterator();
      } else {
        // hash index cannot be used in range query
        isIndexUsable = false;
      }
    } else {
      // b+ tree index can be used in both kinds of query
      isIndexUsable = true;
      if (predicateOperator == ComparisonOperator.EQUAL_TO) {
        indexIterator = FDBHelper.getKVPairIterableWithPrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, isInitializedToLast).iterator();
      } else if (predicateOperator == ComparisonOperator.LESS_THAN) {
        // e.g. Salary < 50
        indexIterator = FDBHelper.getKVPairIterableEndBeforePrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, isInitializedToLast).iterator();
      } else if (predicateOperator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO){
        // e.g. Salary >= 50
        indexIterator = FDBHelper.getKVPairIterableStartWithPrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, isInitializedToLast).iterator();
      } else if (predicateOperator == ComparisonOperator.GREATER_THAN) {
        // e.g. Salary > 50
        indexIterator = FDBHelper.getKVPairIterableWithPrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, true).iterator();
        if (indexIterator.hasNext()) {
          KeyValue kv = indexIterator.next();
          Tuple keyTuple = indexRecordSubspace.unpack(kv.getKey());
          indexIterator.cancel();
          indexIterator = FDBHelper.getKVPairIterableFirstGreaterThanKeyInDirectory(indexRecordSubspace, tx, keyTuple, isInitializedToLast).iterator();
        } else {
          indexIterator.cancel();
          indexIterator = FDBHelper.getKVPairIterableStartWithPrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, isInitializedToLast).iterator();
        }
      } else if (predicateOperator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) {
        // e.g. salary <= 50
        indexIterator = FDBHelper.getKVPairIterableWithPrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, true).iterator();
        if (indexIterator.hasNext()) {
          KeyValue kv = indexIterator.next();
          Tuple keyTuple = indexRecordSubspace.unpack(kv.getKey());
          indexIterator.cancel();
          indexIterator = FDBHelper.getKVPairIterableEndBeforePrefixInDirectory(indexRecordSubspace, tx, keyTuple, isInitializedToLast).iterator();
        } else {
          indexIterator.cancel();
          indexIterator = FDBHelper.getKVPairIterableEndBeforePrefixInDirectory(indexRecordSubspace, tx, indexPrefixQueryTuple, isInitializedToLast).iterator();
        }
      }
    }
  }

  private List<Object> getNextRecordPrimaryKeysUsingIndex() {
    List<Object> res = new ArrayList<>();
    if (indexIterator == null || !indexIterator.hasNext()) {
      return res;
    }

    KeyValue kv = indexIterator.next();
    Tuple keyTuple = indexRecordSubspace.unpack(kv.getKey());

    IndexRecord indexRecord = null;
    if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
      indexRecord = new NonClusteredHashIndexRecord(keyTuple);
    } else if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
      indexRecord = new NonClusteredBPTreeIndexRecord(keyTuple);
    }

    return indexRecord.getPrimaryKeys();
  }

  private CSCI485ClassProject.models.Record getRecordByPrimaryKeys(List<Object> primaryKeys) {
    CSCI485ClassProject.models.Record res = null;
    if (primaryKeys.isEmpty()) {
      return res;
    }
    Tuple primaryKeyTuple = new Tuple();
    for (Object pk : primaryKeys) {
      primaryKeyTuple = primaryKeyTuple.addObject(pk);
    }

    List<KeyValue> recordRawKeyValue = FDBHelper.getKVPairIterableWithPrefixInDirectory(dataRecordSubspace, tx, primaryKeyTuple, false).asList().join();
    List<FDBKVPair> recordFetchedKVPair = new ArrayList<>();
    for (KeyValue kv : recordRawKeyValue) {
      Tuple keyTuple = dataRecordSubspace.unpack(kv.getKey());
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      FDBKVPair kvPair = new FDBKVPair(recordsTransformer.getTableRecordPath(), keyTuple, valTuple);
      recordFetchedKVPair.add(kvPair);
    }

    if (!recordFetchedKVPair.isEmpty()) {
      res = recordsTransformer.convertBackToRecord(recordFetchedKVPair);
    }

    return res;
  }

  private CSCI485ClassProject.models.Record moveToNextRecord(boolean isInitializing) {
    if (!isInitializing && !isInitialized) {
      return null;
    }

    if (isInitializing) {
      // initialize the subspace and the iterator
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      dataRecordSubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(dataRecordSubspace, tx, isInitializedToLast);
      if (fdbIterable != null)
        iterator = fdbIterable.iterator();

      // init the index if needed
      if (isUsingIndex) {
        enableQueryUsingIndex();
      }
      isInitialized = true;

      if (mode == Mode.READ_WRITE) {
        enableIndexMaintenance();
      }
    }
    // reset the currentRecord
    currentRecord = null;

    // no such directory, or no records under the directory
    if (dataRecordSubspace == null || !hasNext()) {
      return null;
    }

    if (isIndexUsable) {
      // use index to get the primary key of the next record
      List<Object> primaryKeys = getNextRecordPrimaryKeysUsingIndex();
      if (primaryKeys.isEmpty()) {
        // reach to EOF
        currentRecord = null;
      } else {
        currentRecord = getRecordByPrimaryKeys(primaryKeys);
      }
    } else {
      List<String> recordStorePath = recordsTransformer.getTableRecordPath();
      List<FDBKVPair> fdbkvPairs = new ArrayList<>();

      boolean isSavePK = false;
      Tuple pkValTuple = new Tuple();
      Tuple tempPkValTuple = null;
      if (isMoved && currentKVPair != null) {
        fdbkvPairs.add(currentKVPair);
        pkValTuple = getPrimaryKeyValTuple(currentKVPair.getKey());
        isSavePK = true;
      }

      isMoved = true;
      boolean nextExists = false;

      while (iterator.hasNext()) {
        KeyValue kv = iterator.next();
        Tuple keyTuple = dataRecordSubspace.unpack(kv.getKey());
        Tuple valTuple = Tuple.fromBytes(kv.getValue());
        FDBKVPair kvPair = new FDBKVPair(recordStorePath, keyTuple, valTuple);
        tempPkValTuple = getPrimaryKeyValTuple(keyTuple);
        if (!isSavePK) {
          pkValTuple = tempPkValTuple;
          isSavePK = true;
        } else if (!pkValTuple.equals(tempPkValTuple)){
          // when pkVal change, stop there
          currentKVPair = kvPair;
          nextExists = true;
          break;
        }
        fdbkvPairs.add(kvPair);
      }
      if (!fdbkvPairs.isEmpty()) {
        currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
      }

      if (!nextExists) {
        currentKVPair = null;
      }
    }
    return currentRecord;
  }

  public CSCI485ClassProject.models.Record getFirst() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = false;

    CSCI485ClassProject.models.Record record = moveToNextRecord(true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  private boolean doesRecordMatchPredicate(CSCI485ClassProject.models.Record record) {
    Object left;
    AttributeType type;
    Object right = null;
    Object coefficient = null;
    ComparisonOperator operator;
    if(comparisonPredicate!=null){
      left = record.getValueForGivenAttrName(comparisonPredicate.getLeftHandSideAttrName());
      type = comparisonPredicate.getLeftHandSideAttrType();
      operator = comparisonPredicate.getOperator();
      //case of 2 attributes in comparison predicate
      if(comparisonPredicate.getPredicateType()==ComparisonPredicate.Type.TWO_ATTRS){
        right = record.getValueForGivenAttrName(comparisonPredicate.getRightHandSideAttrName());
        coefficient = comparisonPredicate.getRightHandSideValue();
      }
      //case of 1 attributes in comparison predicate
      else if(comparisonPredicate.getPredicateType()==ComparisonPredicate.Type.ONE_ATTR){
        right = comparisonPredicate.getRightHandSideValue();
        coefficient = new Integer(1);
      }
    }
    else {
      left = record.getValueForGivenAttrName(predicateAttributeName);
      type = record.getTypeForGivenAttrName(predicateAttributeName);
      if (left == null || type == null) {
        // attribute not exists in this record
        return false;
      }
      operator = predicateOperator;
      right = predicateAttributeValue.getValue();
      coefficient = new Integer(1);
    }
    if (type == AttributeType.INT) {
      return ComparisonUtils.compareTwoINT(left, right, coefficient, operator);
    } else if (type == AttributeType.DOUBLE){
      return ComparisonUtils.compareTwoDOUBLE(left, right, operator);
    } else if (type == AttributeType.VARCHAR) {
      return ComparisonUtils.compareTwoVARCHAR(left, right, operator);
    }
    return false;
  }

  public CSCI485ClassProject.models.Record getLast() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = true;

    CSCI485ClassProject.models.Record record = moveToNextRecord(true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public boolean hasNext() {
    return isInitialized && iterator != null && (iterator.hasNext() || currentKVPair != null);
  }

  public CSCI485ClassProject.models.Record next(boolean isGetPrevious) {
    if (!isInitialized) {
      return null;
    }
    if (isGetPrevious != isInitializedToLast) {
      return null;
    }

    CSCI485ClassProject.models.Record record = moveToNextRecord(false);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public CSCI485ClassProject.models.Record getCurrentRecord() {
    return currentRecord;
  }

  private List<Object> getPrimaryKeysFromCurrentRecord() {
    List<Object> res = new ArrayList<>();
    for (String pkAttr : tableMetadata.getPrimaryKeys()) {
      res.add(currentRecord.getValueForGivenAttrName(pkAttr));
    }
    return res;
  }


  private void deleteOldIndexRecords() {
    List<Object> primaryKeys = getPrimaryKeysFromCurrentRecord();
    if(attrNameToIndexType == null) return;
    for (Map.Entry<String, IndexType> idxPair : attrNameToIndexType.entrySet()) {
      String attrName = idxPair.getKey();
      IndexType attrIdxType = idxPair.getValue();
      Object currentVal = currentRecord.getValueForGivenAttrName(attrName);
      DirectorySubspace directorySubspace = attrNameToIndexSubspace.get(attrName);

      IndexTransformer indexTransformer = new IndexTransformer(tableName, attrName, attrIdxType);
      FDBKVPair kv = indexTransformer.convertToIndexKVPair(attrIdxType, currentVal, primaryKeys);
      FDBHelper.removeKeyValuePair(directorySubspace, tx, kv.getKey());
    }
  }

  private void insertNewIndexRecords() {
    List<Object> primaryKeys = getPrimaryKeysFromCurrentRecord();
    if(attrNameToIndexType==null) return;
    for (Map.Entry<String, IndexType> idxPair : attrNameToIndexType.entrySet()) {
      String attrName = idxPair.getKey();
      IndexType attrIdxType = idxPair.getValue();
      Object newVal = currentRecord.getValueForGivenAttrName(attrName);
      DirectorySubspace directorySubspace = attrNameToIndexSubspace.get(attrName);

      IndexTransformer indexTransformer = new IndexTransformer(tableName, attrName, attrIdxType);
      FDBKVPair kv = indexTransformer.convertToIndexKVPair(attrIdxType, newVal, primaryKeys);

      FDBHelper.setFDBKVPair(directorySubspace, tx, kv);
    }
  }

  public StatusCode updateCurrentRecord(String[] attrNames, Object[] attrValues) {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    Set<String> currentAttrNames = currentRecord.getMapAttrNameToValue().keySet();
    Set<String> primaryKeys = new HashSet<>(tableMetadata.getPrimaryKeys());

    boolean isUpdatingPK = false;
    boolean isUpdatingIndex = false;

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];

      if (!currentAttrNames.contains(attrNameToUpdate)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }

      if (!CSCI485ClassProject.models.Record.Value.isTypeSupported(attrValToUpdate)) {
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }

      if (!isUpdatingPK && primaryKeys.contains(attrNameToUpdate)) {
        isUpdatingPK = true;
      }
    }

    if (isUpdatingPK) {
      // delete the old record first
      StatusCode deleteStatus = deleteCurrentRecord();
      if (deleteStatus != StatusCode.SUCCESS) {
        return deleteStatus;
      }
    }

    deleteOldIndexRecords();

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      currentRecord.setAttrNameAndValue(attrNameToUpdate, attrValToUpdate);
    }

    List<FDBKVPair> kvPairsToUpdate = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToUpdate) {
      FDBHelper.setFDBKVPair(dataRecordSubspace, tx, kv);
    }

    insertNewIndexRecords();

    return StatusCode.SUCCESS;
  }

  public StatusCode deleteCurrentRecord() {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    List<FDBKVPair> kvPairsToDelete = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToDelete) {
      FDBHelper.removeKeyValuePair(dataRecordSubspace, tx, kv.getKey());
    }

    deleteOldIndexRecords();
    return StatusCode.SUCCESS;
  }
}
