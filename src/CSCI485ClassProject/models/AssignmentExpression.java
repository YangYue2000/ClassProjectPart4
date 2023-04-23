package CSCI485ClassProject.models;

import CSCI485ClassProject.StatusCode;

import java.util.HashMap;
import java.util.Map;

import static CSCI485ClassProject.StatusCode.PREDICATE_OR_EXPRESSION_INVALID;

public class AssignmentExpression {
  public enum Type {
    ONE_ATTR, // only one attribute is referenced, e.g. Salary = 1500, Name = "Bob"
    TWO_ATTRS, // two attributes are referenced, e.g. Salary = 1.5 * Age
  }

  private Type expressionType;

  public Type getExpressionType() {
    return expressionType;
  }

  private String leftHandSideAttrName; // e.g. Salary = 1.1 * Age
  public String getLeftAttr() { return leftHandSideAttrName; }
  private AttributeType leftHandSideAttrType;

  // either a specific value, or another attribute
  private Object rightHandSideValue = null; // in the example, it is 1.1
  public Object getRightVal() { return rightHandSideValue; }
  private AlgebraicOperator rightHandSideOperator; // in the example, it is *
  private String rightHandSideAttrName; // in the example, it is Age
  public String getRightAttr(){ return rightHandSideAttrName; }
  private AttributeType rightHandSideAttrType;

  // e.g. Salary = 2000
  public AssignmentExpression(String leftHandSideAttrName, AttributeType leftHandSideAttrType, Object rightHandSideValue) {
    expressionType = Type.ONE_ATTR;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
  }

  // e.g. Salary = Age * 2
  public AssignmentExpression(String leftHandSideAttrName, AttributeType leftHandSideAttrType, String rightHandSideAttrName, AttributeType rightHandSideAttrType, Object rightHandSideValue, AlgebraicOperator rightHandSideOperator) {
    expressionType = Type.TWO_ATTRS;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.rightHandSideAttrName = rightHandSideAttrName;
    this.rightHandSideAttrType = rightHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
    this.rightHandSideOperator = rightHandSideOperator;
  }

  public StatusCode validate() {
    if (expressionType == Type.ONE_ATTR) {
      // e.g. Salary = 2000
      if (leftHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long))
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float))
          || (leftHandSideAttrType == AttributeType.VARCHAR && !(rightHandSideValue instanceof String))) {
        return StatusCode.PREDICATE_OR_EXPRESSION_INVALID;
      }
    } else if (expressionType == Type.TWO_ATTRS) {
      // e.g. Salary = 10 * Age
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
