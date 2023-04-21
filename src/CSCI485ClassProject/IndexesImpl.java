package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.utils.IndexesUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IndexesImpl implements Indexes{

  private Database db;
  private Records records;

  public IndexesImpl(Records records) {
    db = FDBHelper.initialization();
    this.records = records;
  }

  private TableMetadata getTableMetadataByTableName(Transaction tx, String tableName) {
    TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx,
            tblMetadataTransformer.getTableAttributeStorePath());
    TableMetadata tblMetadata = tblMetadataTransformer.convertBackToTableMetadata(kvPairs);
    return tblMetadata;
  }

  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    // your code
    Transaction tx = FDBHelper.openTransaction(db);

    // check if the table exists
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_NOT_FOUND;
    }

    TableMetadata tableMetadata = getTableMetadataByTableName(tx, tableName);
    if (!tableMetadata.getAttributes().containsKey(attrName)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }

    // check if index exists
    IndexTransformer idxTransformer = new IndexTransformer(tableName, attrName, indexType);

    if (IndexesUtils.doesIndexExistOnTableAttribute(tx, tableName, attrName)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }

    // open a cursor to create the index
    // /tableName/indexes/attrName/indexTypeName, e.g. /Employee/indexes/SSN/NON_CLUSTERED_HASH_INDEX
    DirectorySubspace indexesDir = FDBHelper.createOrOpenSubspace(tx, idxTransformer.getIndexStorePath());

    Cursor scanCursor = new Cursor(Cursor.Mode.READ, tableName, tableMetadata, tx);
    boolean isCursorInitialized = false;
    while (true) {
      Record record;
      if (!isCursorInitialized) {
        isCursorInitialized = true;
        record = records.getFirst(scanCursor);
      } else {
        record = records.getNext(scanCursor);
      }
      if (record == null) {
        break;
      }

      Object value = record.getValueForGivenAttrName(attrName);
      if (value != null) {
        List<Object> pkVals = new ArrayList<>();
        for (String pk : tableMetadata.getPrimaryKeys()) {
          pkVals.add(record.getValueForGivenAttrName(pk));
        }
        // insert the index record
        FDBKVPair fdbkvPair = idxTransformer.convertToIndexKVPair(indexType, value, pkVals);
        FDBHelper.setFDBKVPair(indexesDir, tx, fdbkvPair);
      }
    }

    // commit changes
    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    // your code
    Transaction tx = FDBHelper.openTransaction(db);

    // check if the table exists
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_NOT_FOUND;
    }

    TableMetadata tableMetadata = getTableMetadataByTableName(tx, tableName);
    if (!tableMetadata.getAttributes().containsKey(attrName)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }

    if (!IndexesUtils.doesIndexExistOnTableAttribute(tx, tableName, attrName)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.INDEX_NOT_FOUND;
    }

    // delete the whole directory
    FDBHelper.dropSubspace(tx, IndexesUtils.getAttributeIndexDirPath(tableName, attrName));
    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }
}
