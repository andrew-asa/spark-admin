package com.asa.lab.structure.service.spark;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.operator.filter.FilterDriverContent;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;


/**
 * @author andrew_asa
 * @date 2018/10/10.
 */
public class SparkContentManager {

    private static SparkContentManager INSTANCE;

    private SparkConfigure configure;

    private SparkSession session;

    private SparkContentManager() {

        init();
    }

    private void init() {

        configure = new SparkConfigure();
    }

    public void setDefault() {

        DataSourceDriverContent.getInstance().registerDefaultDriver();
        FilterDriverContent.getInstance().setDefaultDriver();
    }

    public void addConfigure(String key, String value) {

        configure.setConfigure(key, value);
    }

    public static SparkContentManager getInstance() {

        if (INSTANCE == null) {
            synchronized (SparkContentManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new SparkContentManager();
                }
            }
        }
        return INSTANCE;
    }

    public synchronized SparkSession getSession() {

        if (session == null) {
            SparkSession.Builder builder = SparkSession
                    .builder()
                    .master(configure.getMaster())
                    .appName(configure.getAppName());
            session = builder.getOrCreate();
            setUpProperties(builder);
        }
        return session;
    }

    private void setUpProperties(SparkSession.Builder builder) {

    }


    public SparkContext sparkContext() {

        return getSession().sparkContext();
    }

    public void closeSession() {

        SparkSession session = getSession();
        if (session != null) {
            session.close();
        }
        this.session = null;
    }
}
