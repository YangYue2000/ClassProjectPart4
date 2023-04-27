package CSCI485ClassProject.test;

import CSCI485ClassProject.Indexes;
import CSCI485ClassProject.IndexesImpl;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.RelationalAlgebraOperators;
import CSCI485ClassProject.RelationalAlgebraOperatorsImpl;
import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.TableManager;
import CSCI485ClassProject.TableManagerImpl;
import CSCI485ClassProject.models.AlgebraicOperator;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class Part4Test {

  public static String EmployeeTableName = "Employee";
  public static String SSN = "SSN";
  public static String Name = "Name";
  public static String Email = "Email";
  public static String Age = "Age";
  public static String Address = "Address";
  public static String Salary = "Salary";

  public static String DepartmentTableName = "Department";
  public static String DNO = "DNO";
  public static String Floor = "Floor";

  public static String[] EmployeeTableAttributeNames =
      new String[]{SSN, DNO, Name, Email, Age, Address, Salary};
  public static AttributeType[] EmployeeTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.INT, AttributeType.VARCHAR,
          AttributeType.VARCHAR, AttributeType.INT, AttributeType.VARCHAR, AttributeType.INT};
  public static String[] EmployeeTablePKAttributes =
      new String[]{SSN};
  public static String[] EmployeeTableNonPKAttributeNames =
      new String[]{DNO, Name, Email, Age, Address, Salary};

  public static String[] DepartmentTableAttributeNames =
      new String[]{DNO, Name, Floor};
  public static AttributeType[] DepartmentTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR, AttributeType.INT};
  public static String[] DepartmentTablePKAttributes =
      new String[]{DNO};
  public static String[] DepartmentTableNonPKAttributeNames =
      new String[]{Name, Floor};

  public static int initialNumberOfRecords = 100;
  public static int updatedNumberOfRecords = 100;
  public static int dnoLB = 20;
  public static int dnoUB = 80;
  public static int randSeed = 10;


  private TableManager tableManager;
  private Records records;
  private Indexes indexes;
  private RelationalAlgebraOperators relAlgOperators;

  private String getName(long i) {
    return "Name" + i;
  }

  private String getEmail(long i) {
    return "ABCDEFGH" + i + "@usc.edu";
  }

  private long getAge(long i) {
    return 20 + i / 10;
  }

  private String getAddress(long i) {
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + i;
  }

  private long getSalary(long i) {
    return i;
  }

  private String getDepartmentName(long i) {
    return "Department" + i;
  }

  private long getFloor(long i) {
    return i;
  }

  private long getDno(Random generator, long lowerBound, long upperBound) {
    long range = upperBound - lowerBound + 1;
    long randomLong = generator.nextLong() % range;

    if (randomLong < 0) {
      randomLong = -randomLong;
    }
    return randomLong + lowerBound;
  }


  @Before
  public void init(){
    tableManager = new TableManagerImpl();
    records = new RecordsImpl();
    indexes = new IndexesImpl(records);
    relAlgOperators = new RelationalAlgebraOperatorsImpl();
  }

  private Record getExpectedDepartmentRecord(long dno) {
    Record rec = new Record();
    String name = getDepartmentName(dno);
    long floor = getFloor(dno);

    rec.setAttrNameAndValue(DNO, dno);
    rec.setAttrNameAndValue(Name, name);
    rec.setAttrNameAndValue(Floor, floor);

    return rec;
  }

  private Record getExpectedEmployeeRecord(long ssn, long dno) {
    Record rec = new Record();
    String name = getName(ssn);
    String email = getEmail(ssn);
    long age = getAge(ssn);
    String address = getAddress(ssn);
    long salary = getSalary(ssn);

    rec.setAttrNameAndValue(SSN, ssn);
    rec.setAttrNameAndValue(DNO, dno);
    rec.setAttrNameAndValue(Name, name);
    rec.setAttrNameAndValue(Email, email);
    rec.setAttrNameAndValue(Age, age);
    rec.setAttrNameAndValue(Address, address);
    rec.setAttrNameAndValue(Salary, salary);

    return rec;
  }

  private Record getExpectedJoinedEmpDepRecord(Record employee, Record department) {
    Record res = new Record();
    res.setAttrNameAndValue(SSN, employee.getValueForGivenAttrName(SSN));
    res.setAttrNameAndValue(Email, employee.getValueForGivenAttrName(Email));
    res.setAttrNameAndValue(Age, employee.getValueForGivenAttrName(Age));
    res.setAttrNameAndValue(EmployeeTableName + "." + Name, employee.getValueForGivenAttrName(Name));
    res.setAttrNameAndValue(EmployeeTableName + "." + DNO, employee.getValueForGivenAttrName(DNO));
    res.setAttrNameAndValue(Address, employee.getValueForGivenAttrName(Address));
    res.setAttrNameAndValue(Salary, employee.getValueForGivenAttrName(Salary));
    res.setAttrNameAndValue(Floor, department.getValueForGivenAttrName(Floor));
    res.setAttrNameAndValue(DepartmentTableName + "." + DNO, department.getValueForGivenAttrName(DNO));
    res.setAttrNameAndValue(DepartmentTableName + "." + Name, department.getValueForGivenAttrName(Name));

    return res;
  }

  @Test
  public void unitTest1() {
    tableManager.dropAllTables();
    // create the Employee Table, verify that the table is created
    TableMetadata EmployeeTable = new TableMetadata(EmployeeTableAttributeNames, EmployeeTableAttributeTypes,
        EmployeeTablePKAttributes);
    assertEquals(StatusCode.SUCCESS, tableManager.createTable(EmployeeTableName,
        EmployeeTableAttributeNames, EmployeeTableAttributeTypes, EmployeeTablePKAttributes));
    HashMap<String, TableMetadata> tables = tableManager.listTables();
    assertEquals(1, tables.size());
    assertEquals(EmployeeTable, tables.get(EmployeeTableName));

    Random randGenerator = new Random(randSeed);
    Set<Record> expectSet = new HashSet<>();
    for (int i = 0; i < initialNumberOfRecords; i++) {
      long ssn = i;
      long dno = getDno(randGenerator, dnoLB, dnoUB);
      String name = getName(i);
      String email = getEmail(i);
      long age = getAge(i);
      String address = getAddress(i);
      long salary = getSalary(i);

      if (salary < age * 2) {
        expectSet.add(getExpectedEmployeeRecord(ssn, dno));
      }
      Object[] primaryKeyVal = new Object[] {ssn};
      Object[] nonPrimaryKeyVal = new Object[] {dno, name, email, age, address, salary};

      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, EmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
    }

    ComparisonPredicate predicate = new ComparisonPredicate(Salary, AttributeType.INT, ComparisonOperator.LESS_THAN_OR_EQUAL_TO, 25);
    Iterator selectRes = relAlgOperators.select(EmployeeTableName, predicate, Iterator.Mode.READ, false);

    assertNotNull(selectRes);
    randGenerator = new Random(randSeed);
    for (int i = 0; i <= 25; i++) {
      long ssn = i;
      long dno = getDno(randGenerator, dnoLB, dnoUB);
      Record expectRecord = getExpectedEmployeeRecord(ssn, dno);
      Record actualRecord = selectRes.next();

      assertEquals(expectRecord, actualRecord);
    }
    assertNull(selectRes.next());
    selectRes.commit();

    predicate =
        new ComparisonPredicate(Salary, AttributeType.INT, ComparisonOperator.LESS_THAN,
            Age, AttributeType.INT, 2, AlgebraicOperator.PRODUCT);
    Set<Record> actualSelectSet = relAlgOperators.simpleSelect(EmployeeTableName, predicate, false);
    assertEquals(expectSet, actualSelectSet);

    predicate =
        new ComparisonPredicate(Salary, AttributeType.INT, ComparisonOperator.LESS_THAN,
            Name, AttributeType.VARCHAR, 2, AlgebraicOperator.PRODUCT);
    assertNull(relAlgOperators.select(EmployeeTableName, predicate, Iterator.Mode.READ_WRITE, false));

    System.out.println("Test1 passed!");
  }


  @Test
  public void unitTest2 () {
    Iterator projectIterator = relAlgOperators.project(EmployeeTableName, DNO, false);
    Random randomGenerator = new Random(randSeed);

    Set<Long> expectDnoSet = new HashSet<>();
    for (int i = 0; i<initialNumberOfRecords; i++) {
      long dno = getDno(randomGenerator, dnoLB, dnoUB);
      Record record = projectIterator.next();
      assertNotNull(record);

      Record expectRecord = new Record();
      expectRecord.setAttrNameAndValue(DNO, dno);

      assertEquals(dno, record.getValueForGivenAttrName(DNO));
      expectDnoSet.add(dno);
    }

    List<Record> inorderDnoRecords = relAlgOperators.simpleProject(EmployeeTableName, DNO, true);
    List<Long> actualDnoList = new ArrayList<>();

    for (Record record : inorderDnoRecords) {
      actualDnoList.add((Long) record.getValueForGivenAttrName(DNO));
    }

    List<Long> expectDnoList = new ArrayList<>(expectDnoSet);
    java.util.Collections.sort(expectDnoList);
    assertEquals(expectDnoList, actualDnoList);

    ComparisonPredicate predicate = new ComparisonPredicate(SSN, AttributeType.INT, ComparisonOperator.LESS_THAN, 50);
    Iterator selectRes = relAlgOperators.select(EmployeeTableName, predicate, Iterator.Mode.READ_WRITE, false);
    assertNotNull(selectRes);


    Iterator emailRecordIterator = relAlgOperators.project(selectRes, Email, true);
    Set<String> expectedEmailSet = new HashSet<>();
    Set<String> actualEmailSet = new HashSet<>();

    for (int i = 0; i < 50; i++) {
      Record record = emailRecordIterator.next();
      assertNotNull(record);

      expectedEmailSet.add(getEmail(i));
      actualEmailSet.add((String) record.getValueForGivenAttrName(Email));
    }

    assertNull(emailRecordIterator.next());
    emailRecordIterator.commit();

    assertEquals(expectedEmailSet, actualEmailSet);
    System.out.println("Test2 passed!");
  }

  @Test
  public void unitTest3 () {
    // create the Department Table
    assertEquals(StatusCode.SUCCESS, tableManager.createTable(DepartmentTableName,
        DepartmentTableAttributeNames, DepartmentTableAttributeTypes, DepartmentTablePKAttributes));

    for (int i = 0; i < initialNumberOfRecords; i++) {
      long dno = i;
      long floor = getFloor(i);
      String name = getDepartmentName(i);

      Object[] primaryKeyVal = new Object[] {dno};
      Object[] nonPrimaryKeyVal = new Object[] {name, floor};

      assertEquals(StatusCode.SUCCESS, records.insertRecord(DepartmentTableName, DepartmentTablePKAttributes, primaryKeyVal, DepartmentTableNonPKAttributeNames, nonPrimaryKeyVal));
    }

    ComparisonPredicate nonePredicate = new ComparisonPredicate();
    Iterator employeeIterator = relAlgOperators.select(EmployeeTableName, nonePredicate, Iterator.Mode.READ, false);
    Iterator departmentIterator = relAlgOperators.select(DepartmentTableName, nonePredicate, Iterator.Mode.READ, false);

    ComparisonPredicate joinPredicate =
        new ComparisonPredicate(DNO, AttributeType.INT, ComparisonOperator.EQUAL_TO, DNO, AttributeType.INT, 1, AlgebraicOperator.PRODUCT);
    Iterator joinResIterator = relAlgOperators.join(employeeIterator, departmentIterator, joinPredicate, null);

    // construct the expect record set
    Set<Record> expectedRecordSet = new HashSet<>();
    Random randomGenerator = new Random(randSeed);
    for (int i = 0; i < initialNumberOfRecords; i++) {
      long ssn = i;
      long dno = getDno(randomGenerator, dnoLB, dnoUB);
      Record employeeRecord = getExpectedEmployeeRecord(ssn, dno);
      Record departmentRecord = getExpectedDepartmentRecord(dno);
      Record joinedRecord = getExpectedJoinedEmpDepRecord(employeeRecord, departmentRecord);

      expectedRecordSet.add(joinedRecord);
    }


    Set<Record> actualRecordSet = new HashSet<>();
    while (true) {
      Record record = joinResIterator.next();
      if (record == null) {
        break;
      }
      actualRecordSet.add(record);
    }

    assertEquals(expectedRecordSet, actualRecordSet);
    joinResIterator.commit();
    System.out.println("Test3 passed!");
  }

  @Test
  public void unitTest4() {
    // insert new records in the department table
    for (int i = initialNumberOfRecords; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
      Record record = getExpectedDepartmentRecord(i);
      assertEquals(StatusCode.SUCCESS, relAlgOperators.insert(DepartmentTableName, record, DepartmentTablePKAttributes));
    }

    Iterator departmentIterator = relAlgOperators.select(DepartmentTableName, new ComparisonPredicate(), Iterator.Mode.READ, false);
    assertNotNull(departmentIterator);

    for (int i = 0; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
      Record record = departmentIterator.next();
      Record expectedRecord = getExpectedDepartmentRecord(i);
      assertEquals(expectedRecord, record);
    }

    assertNull(departmentIterator.next());
    System.out.println("Test4 passed!");
  }

  @Test
  public void unitTest5() {
    AssignmentExpression salaryUpdateExpression =
        new AssignmentExpression(Salary, AttributeType.INT, Salary, AttributeType.INT, 2, AlgebraicOperator.PRODUCT);
    assertEquals(StatusCode.SUCCESS, relAlgOperators.update(EmployeeTableName, salaryUpdateExpression, null));

    // verify the updates
    Iterator iterator = relAlgOperators.select(EmployeeTableName, new ComparisonPredicate(), Iterator.Mode.READ, false);
    for (int i = 0; i < initialNumberOfRecords; i++) {
      long ssn = i;
      long salary = 2 * getSalary(ssn);

      Record record = iterator.next();
      assertNotNull(record);
      assertEquals(salary, record.getValueForGivenAttrName(Salary));
    }

    assertNull(iterator.next());
    iterator.commit();

    // get all employees that salary <= 80
    ComparisonPredicate compPredicate =
        new ComparisonPredicate(Salary, AttributeType.INT, ComparisonOperator.LESS_THAN_OR_EQUAL_TO, 80);
    Iterator employeeIterator = relAlgOperators.select(EmployeeTableName, compPredicate, Iterator.Mode.READ_WRITE, false);
    assertNotNull(employeeIterator);

    // make their salary = 4 * age
    salaryUpdateExpression =
        new AssignmentExpression(Salary, AttributeType.INT, Age, AttributeType.INT, 4, AlgebraicOperator.PRODUCT);

    assertEquals(StatusCode.SUCCESS, relAlgOperators.update(EmployeeTableName, salaryUpdateExpression, employeeIterator));
    // verify the updates
    iterator = relAlgOperators.select(EmployeeTableName, new ComparisonPredicate(), Iterator.Mode.READ, false);
    for (int i = 0; i < initialNumberOfRecords; i++) {
      long ssn = i;
      long salary = 2 * getSalary(ssn);
      long age = getAge(ssn);

      Record record = iterator.next();
      assertNotNull(record);

      if (salary <= 80) {
        assertEquals(4 * age, record.getValueForGivenAttrName(Salary));
      } else {
        assertEquals(salary, record.getValueForGivenAttrName(Salary));
      }
    }

    assertNull(iterator.next());
    iterator.commit();
    System.out.println("Test5 passed!");
  }

  @Test
  public void unitTest6() {
    // delete Department with DNO=40
    ComparisonPredicate dnoEq40predicate =
        new ComparisonPredicate(DNO, AttributeType.INT, ComparisonOperator.EQUAL_TO, 40);

    Iterator departmentIterator = relAlgOperators.select(DepartmentTableName, dnoEq40predicate, Iterator.Mode.READ_WRITE, false);
    assertNotNull(departmentIterator);

    assertEquals(StatusCode.SUCCESS, relAlgOperators.delete(DepartmentTableName, departmentIterator));

    // verify the deletion
    departmentIterator = relAlgOperators.select(DepartmentTableName, dnoEq40predicate, Iterator.Mode.READ, false);
    assertNotNull(departmentIterator);
    assertNull(departmentIterator.next());
    departmentIterator.commit();


    Iterator employeeIterator = relAlgOperators.select(EmployeeTableName, dnoEq40predicate, Iterator.Mode.READ_WRITE, false);
    assertNotNull(employeeIterator);

    assertEquals(StatusCode.SUCCESS, relAlgOperators.delete(EmployeeTableName, employeeIterator));

    // verify the deletion
    employeeIterator = relAlgOperators.select(EmployeeTableName, dnoEq40predicate, Iterator.Mode.READ, false);
    assertNotNull(employeeIterator);
    assertNull(employeeIterator.next());
    employeeIterator.commit();

    System.out.println("Test6 passed!");
  }
}
