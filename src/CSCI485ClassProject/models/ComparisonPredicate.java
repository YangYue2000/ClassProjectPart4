package CSCI485ClassProject.models;

import CSCI485ClassProject.StatusCode;

import static CSCI485ClassProject.StatusCode.PREDICATE_OR_EXPRESSION_INVALID;

public class ComparisonPredicate {

  public enum Type {
    NONE, // meaning no predicate
    ONE_ATTR, // only one attribute is referenced, e.g. Salary < 1500, Name == "Bob"
    TWO_ATTRS, // two attributes are referenced, e.g. Salary >= 1.5 * Age
  }

  private Type predicateType = Type.NONE;

  public Type getPredicateType() {
    return predicateType;
  }

  private String leftHandSideAttrName; // e.g. Salary == 1.1 * Age
  private AttributeType leftHandSideAttrType;

  ComparisonOperator operator; // in the example, it is ==

  // either a specific value, or another attribute
  private Object rightHandSideValue = null; // in the example, it is 1.1
  private AlgebraicOperator rightHandSideOperator; // in the example, it is *

  private String rightHandSideAttrName; // in the example, it is Age
  private AttributeType rightHandSideAttrType;

  public String getLeftHandSideAttrName() {
    return leftHandSideAttrName;
  }
  public String getRightHandSideAttrName() {
    return rightHandSideAttrName;
  }

  public void setLeftHandSideAttrName(String leftHandSideAttrName) {
    this.leftHandSideAttrName = leftHandSideAttrName;
  }

  public AttributeType getLeftHandSideAttrType() {
    return leftHandSideAttrType;
  }

  public void setLeftHandSideAttrType(AttributeType leftHandSideAttrType) {
    this.leftHandSideAttrType = leftHandSideAttrType;
  }

  public ComparisonOperator getOperator() {
    return operator;
  }

  public void setOperator(ComparisonOperator operator) {
    this.operator = operator;
  }

  public Object getRightHandSideValue() {
    return rightHandSideValue;
  }

  public void setRightHandSideValue(Object rightHandSideValue) {
    this.rightHandSideValue = rightHandSideValue;
  }

  public ComparisonPredicate() {
    // None predicate by default
  }
  // e.g. Salary == 10000, Salary <= 5000
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, Object rightHandSideValue) {
    predicateType = Type.ONE_ATTR;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.operator = operator;
    this.rightHandSideValue = rightHandSideValue;
  }

  // e.g. Salary == 1.1 * Age
  public ComparisonPredicate(String leftHandSideAttrName, AttributeType leftHandSideAttrType, ComparisonOperator operator, String rightHandSideAttrName, AttributeType rightHandSideAttrType, Object rightHandSideValue, AlgebraicOperator rightHandSideOperator) {
    predicateType = Type.TWO_ATTRS;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.operator = operator;
    this.rightHandSideAttrName = rightHandSideAttrName;
    this.rightHandSideAttrType = rightHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
    this.rightHandSideOperator = rightHandSideOperator;
  }

  // validate the predicate, return PREDICATE_VALID if the predicate is valid
  public StatusCode validate() {
    if (predicateType == Type.NONE) {
      return StatusCode.PREDICATE_OR_EXPRESSION_VALID;
    } else if (predicateType == Type.ONE_ATTR) {
      // e.g. Salary > 2000
      if (leftHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long))
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float))
          || (leftHandSideAttrType == AttributeType.VARCHAR && !(rightHandSideValue instanceof String))) {
          return StatusCode.PREDICATE_OR_EXPRESSION_INVALID;
      }
    } else if (predicateType == Type.TWO_ATTRS) {
      // e.g. Salary >= 10 * Age
      if (leftHandSideAttrType == AttributeType.NULL || rightHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.VARCHAR || rightHandSideAttrType == AttributeType.VARCHAR)
          || (leftHandSideAttrType != rightHandSideAttrType)
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long)
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float)))) {
        return PREDICATE_OR_EXPRESSION_INVALID;
      }
    }
    return StatusCode.PREDICATE_OR_EXPRESSION_VALID;
  }
}
