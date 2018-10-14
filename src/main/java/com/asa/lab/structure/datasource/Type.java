package com.asa.lab.structure.datasource;

import java.io.Serializable;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public enum Type implements Serializable {
    Bool(java.lang.Boolean.class),
    Int(java.lang.Integer.class),
    Float(java.lang.Float.class),
    Long(java.lang.Long.class),
    Double(java.lang.Double.class),
    String(java.lang.String.class),
    Date(java.sql.Date.class),
    Time(java.sql.Time.class),
    Timestamp(java.sql.Timestamp.class),
    Decimal(java.math.BigDecimal.class);

    private Class typeClass;

    Type(Class typeClass) {

        this.typeClass = typeClass;
    }

    public Class getTypeClass() {

        return this.typeClass;
    }

    public <T> T accept(TypeVisitor<T> visitor) {

        switch (this) {
            case Bool:
                return visitor.visitBool();
            case Int:
                return visitor.visitInt();
            case Float:
                return visitor.visitFloat();
            case Long:
                return visitor.visitLong();
            case Double:
                return visitor.visitDouble();
            case String:
                return visitor.visitString();
            case Date:
                return visitor.visitDate();
            case Time:
                return visitor.visitTime();
            case Timestamp:
                return visitor.visitTimestamp();
            default:
                throw new RuntimeException();
        }
    }

    public void acceptVoid(TypeVoidVisitor visitor) {

        switch (this) {
            case Bool:
                visitor.visitBool();
            case Int:
                visitor.visitInt();
            case Float:
                visitor.visitFloat();
            case Long:
                visitor.visitLong();
            case Double:
                visitor.visitDouble();
            case String:
                visitor.visitString();
            case Date:
                visitor.visitDate();
            case Time:
                visitor.visitTime();
            case Timestamp:
                visitor.visitTimestamp();
            default:
                throw new RuntimeException();
        }
    }

    public int toInt() {

        switch (this) {
            case Bool:
                return 1;
            case Int:
                return 2;
            case Float:
                return 3;
            case Long:
                return 4;
            case Double:
                return 5;
            case String:
                return 6;
            case Date:
                return 7;
            case Time:
                return 8;
            case Timestamp:
                return 9;
            case Decimal:
                return 10;
            default:
                throw new RuntimeException();
        }
    }

    public static Type fromInt(int i) {

        switch (i) {
            case 1:
                return Type.Bool;
            case 2:
                return Type.Int;
            case 3:
                return Type.Float;
            case 4:
                return Type.Long;
            case 5:
                return Type.Double;
            case 6:
                return Type.String;
            case 7:
                return Type.Date;
            case 8:
                return Type.Time;
            case 9:
                return Type.Timestamp;
            case 10:
                return Type.Decimal;
            default:
                throw new RuntimeException(i + "");
        }
    }


    @Override
    public java.lang.String toString() {

        return "Type{" +
                "typeClass=" + typeClass +
                '}';
    }
}
