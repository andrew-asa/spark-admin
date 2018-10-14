package com.asa.lab.structure.datasource;

/**
 * Created by andrew_asa on 2018/8/3.
 */
public interface TypeVisitor<T> {

    T visitBool();

    T visitInt();

    T visitFloat();

    T visitLong();

    T visitDouble();

    T visitString();

    T visitDate();

    T visitTime();

    T visitTimestamp();
}
