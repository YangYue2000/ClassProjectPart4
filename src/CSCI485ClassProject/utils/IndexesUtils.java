package CSCI485ClassProject.utils;

import CSCI485ClassProject.DBConf;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IndexesUtils {

  public static List<String> getAttributeIndexDirPath(String tableName, String attrName) {
    List<String> dirPath = new ArrayList<>();
    dirPath.add(tableName);
    dirPath.add(DBConf.TABLE_INDEXES_STORE);
    dirPath.add(attrName);

    return dirPath;
  }

  public static boolean doesIndexExistOnTableAttribute(Transaction tx, String tableName, String attrName) {
    return FDBHelper.doesSubdirectoryExists(tx, getAttributeIndexDirPath(tableName, attrName));
  }

  public static IndexType getIndexTypeOfTableAttribute(Transaction tx, String tableName, String attrName) {
    List<String> dirPath = new ArrayList<>();
    dirPath.add(tableName);
    dirPath.add(DBConf.TABLE_INDEXES_STORE);
    dirPath.add(attrName);

    List<String> indexDirs = FDBHelper.getAllDirectSubspacesNameUnderGivenPath(tx, dirPath);
    String indexTypeName = indexDirs.get(0);
    IndexType idxType = IndexType.valueOf(indexTypeName);

    return idxType;
  }

  public static IndexType getIndexTypeOfTableAttribute(Transaction tx, DirectorySubspace idxSpace) {
    List<String> indexDirs = idxSpace.getPath();
    String indexTypeName = indexDirs.get(indexDirs.size()-1);
    IndexType idxType = IndexType.valueOf(indexTypeName);
    return idxType;
  }

  public static HashMap<String, DirectorySubspace> openIndexSubspacesOfTable(Transaction tx, String tableName, TableMetadata tblMetadata) {
    List<String> dirPath = new ArrayList<>();
    dirPath.add(tableName);
    dirPath.add(DBConf.TABLE_INDEXES_STORE);

    HashMap<String, DirectorySubspace> res = new HashMap<>();
    for (String attrName : tblMetadata.getAttributes().keySet()) {
      if (doesIndexExistOnTableAttribute(tx, tableName, attrName)) {
        List<String> attrIndexPath = new ArrayList<>(dirPath);
        attrIndexPath.add(attrName);

        List<String> indexDirs = FDBHelper.getAllDirectSubspacesNameUnderGivenPath(tx, attrIndexPath);
        String indexTypeName = indexDirs.get(0);
        attrIndexPath.add(indexTypeName);

        DirectorySubspace indexSpace = FDBHelper.openSubspace(tx, attrIndexPath);
        res.put(attrName, indexSpace);
      }
    }
    return res;
  }
}
