package com.asa.lab.expection;

import com.asa.utils.StringUtils;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 */
public class DSUnSupportException extends RuntimeException {

    public DSUnSupportException(String format, Object... args) {

        super(StringUtils.messageFormat(format, args));
    }
}
