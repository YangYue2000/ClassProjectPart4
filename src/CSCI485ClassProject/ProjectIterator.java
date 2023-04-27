package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.List;
import java.util.Random;

public class ProjectIterator extends Iterator{
    private String tableName = "";
    private final String attrName;
    private final boolean isDuplicatedFree;
    private SelectIterator inputIterator = null;
    private String projectTableName = "";
    private final SelectIterator projectIterator;
    private RecordsImpl records;;
    private final ComparisonPredicate nonePredicate = new ComparisonPredicate();
    private final Mode mode = Mode.READ;
    private final boolean isUsingIndex = false;
    ProjectIterator(String tableName, String attrName, boolean isDuplicateFree){
        this.inputIterator = new SelectIterator(tableName,nonePredicate,mode,isUsingIndex);
        this.tableName = tableName;
        this.attrName = attrName;
        this.isDuplicatedFree = isDuplicateFree;
        if(isDuplicateFree){
            //create project table
            String[] attributeNames = new String[1];
            attributeNames[0] = this.attrName;
            AttributeType[] attributeType = new AttributeType[1];
            attributeType[0] = AttributeType.INT;
            Random rand = new Random();
            long timestamp = System.currentTimeMillis();
            this.projectTableName = timestamp+this.tableName;
            TableManagerImpl tableManager = new TableManagerImpl();
            StatusCode createProjectTableStatus = tableManager.createTable(this.projectTableName, attributeNames, attributeType,
                    attributeNames);
            if(createProjectTableStatus != StatusCode.SUCCESS){
                System.out.println("create project table faild: "+createProjectTableStatus);
            }
            //use a select iterator to get all values from input table;
            records = new RecordsImpl();
            while (true) {
                Record record = inputIterator.next();
                if (record == null) {
                    break;
                }
                Object[] val = new Object[1];
                val[0] = record.getValueForGivenAttrName(this.attrName);
                StatusCode insertToProjectTableStatus = records.insertRecord(projectTableName, attributeNames, val, attributeNames, val);
            }
            projectIterator = new SelectIterator(projectTableName,nonePredicate,mode,isUsingIndex);
        }
        else{
            projectIterator = this.inputIterator;
        }
    }
    ProjectIterator(Iterator inputIterator, String attrName, boolean isDuplicateFree){
        this.inputIterator = (SelectIterator) inputIterator;
        this.attrName = attrName;
        this.isDuplicatedFree = isDuplicateFree;
        if(isDuplicateFree){
            //create project table
            String[] attributeNames = new String[1];
            attributeNames[0] = this.attrName;
            AttributeType[] attributeType = new AttributeType[1];
            attributeType[0] = AttributeType.VARCHAR;
            long timestamp = System.currentTimeMillis();
            this.projectTableName = timestamp+this.inputIterator.getTableNameOfRecord();
            TableManagerImpl tableManager = new TableManagerImpl();
            StatusCode createProjectTableStatus = tableManager.createTable(this.projectTableName, attributeNames, attributeType,
                    attributeNames);
            if(createProjectTableStatus != StatusCode.SUCCESS){
                System.out.println("create project table faild: "+createProjectTableStatus);
            }
            //use a select iterator to get all values from input table;
            records = new RecordsImpl();
            while (true) {
                Record record = inputIterator.next();
                if (record == null) {
                    break;
                }
                Object[] val = new Object[1];
                val[0] = record.getValueForGivenAttrName(attrName);
                StatusCode insertToProjectTableStatus = records.insertRecord(projectTableName, attributeNames, val, attributeNames, val);
            }
            projectIterator = new SelectIterator(projectTableName,nonePredicate,mode,isUsingIndex);
        }
        else{
            projectIterator =  this.inputIterator;
        }
    }
    @Override
    public Record next() {
        if(isDuplicatedFree){
            return projectIterator.next();
        }
        else{
            Record originalRecord = projectIterator.next();
            if(originalRecord==null) return originalRecord;
            Record projectRecord = new Record();
            projectRecord.setAttrNameAndValue(attrName, originalRecord.getValueForGivenAttrName(attrName));
            return projectRecord;
        }
    }

    @Override
    public void commit() {
        inputIterator.commit();
        projectIterator.commit();
    }

    @Override
    public void abort() {
        inputIterator.abort();
        projectIterator.abort();
    }
}
