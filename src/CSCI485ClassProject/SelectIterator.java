package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

public class SelectIterator extends Iterator{
    private final RecordsImpl records;
    private boolean first;
    private final String tableName;
    private final ComparisonPredicate predicate;
    private final Iterator.Mode mode;
    private final boolean isUsingIndex;
    SelectIterator(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex){
        this.tableName = tableName;
        this.predicate = predicate;
        this.mode = mode;
        this.isUsingIndex = isUsingIndex;
        records = new RecordsImpl();
        first = true;
        Cursor.Mode cursor_mode = null;
        if(mode==Iterator.Mode.READ){
            cursor_mode = Cursor.Mode.READ;
        }
        else if(mode== Iterator.Mode.READ_WRITE){
            cursor_mode = Cursor.Mode.READ_WRITE;
        }
        if(predicate.getPredicateType()==ComparisonPredicate.Type.NONE){
            cursor = records.openCursor(tableName,cursor_mode);
        }
        else{
            cursor = records.openCursor(tableName,predicate,cursor_mode,isUsingIndex);
        }
    }
    public Record first(){
        return records.getFirst(cursor);
    }
    @Override
    public Record next() {
        if(first){
            first = false;
            return first();
        }
        else {
            return records.getNext(cursor);
        }
    }

    @Override
    public void commit() {
        records.commitCursor(cursor);
    }

    @Override
    public void abort() {
        records.abortCursor(cursor);
    }

    public String getTableNameOfRecord(){
        return tableName;
    }

    public ComparisonPredicate getPredicate(){
        return predicate;
    }

    public Iterator.Mode getmode(){
        return mode;
    }

    public boolean getIsUsingIndex(){
        return isUsingIndex;
    }

}
