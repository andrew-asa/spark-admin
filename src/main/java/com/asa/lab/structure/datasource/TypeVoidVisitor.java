package com.asa.lab.structure.datasource;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public interface TypeVoidVisitor {

    void visitBool();

    void visitInt();

    void visitFloat();

    void visitLong();

    void visitDouble();

    void visitString();

    void visitDate();

    void visitTime();

    void visitTimestamp();
}
