package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;

public abstract class Iterator {
  protected Cursor cursor;
  public enum Mode {
    READ,
    READ_WRITE
  }

  private Mode mode;

  public Mode getMode() {
    return mode;
  };

  public void setMode(Mode mode) {
    this.mode = mode;
  };

  public abstract Record next();

  public abstract void commit();

  public abstract void abort();

  public Cursor getCursor() { return cursor; }
}
