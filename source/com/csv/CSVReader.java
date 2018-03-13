package com.csv;

import com.csv.util.CSVUtil;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CSVReader
{
    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:mem:dbukdb;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "CSVReader";
    private static final String DB_PASSWORD = "CSVReader";

    private static volatile DataSource DATA_SOURCE = null;

    private static class Lock
    {
    }

    private static Connection getDBConnection() throws ClassNotFoundException, SQLException
    {

        if (DATA_SOURCE == null)
        {
            synchronized (Lock.class)
            {
                if (DATA_SOURCE == null)
                {
                    try
                    {
                        Class.forName(DB_DRIVER);
                    } catch (ClassNotFoundException e)
                    {
                        System.out.println("Unable to start a database..");
                        System.out.println(e.getMessage());
                        throw e;
                    }

                    JdbcDataSource ds = new JdbcDataSource();
                    ds.setURL(DB_CONNECTION);
                    ds.setUser(DB_USER);
                    ds.setPassword(DB_PASSWORD);
                    DATA_SOURCE = ds;
                }
            }
        }

        return DATA_SOURCE.getConnection();
    }

    public static String importFile(String fromFile) throws Exception
    {
        String tableName = CSVUtil.getTableNameFromCSVFile(fromFile);
        importFile(fromFile, tableName);
        return tableName;
    }

    public static void importFile(String fromFile, String targetTableName) throws Exception
    {
        String query = CSVConstants.IMPORT_QUERY.replaceAll(CSVConstants.TABLE_NAME, targetTableName);
        query = query.replaceAll(CSVConstants.FILE_PATH, fromFile);
        execute(query);
    }

    public static void dropTable(String tableName) throws Exception
    {
        String query = CSVConstants.DROP_QUERY.replaceAll(CSVConstants.TABLE_NAME, tableName);
        execute(query);
    }

    private static void execute(String query) throws Exception
    {
        executeQuery(query, false);
    }

    public static ResultData get(String query) throws Exception
    {
        return executeQuery(query, true);
    }

    public static ResultData getUsingPreparedStatement(String query, Object... parameters) throws Exception
    {
        return executeQuery(query, true, parameters);
    }

    private static ResultData executeQuery(String query, boolean isReadOnly, Object... parameters) throws Exception
    {
        Connection con = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try
        {
            con = getDBConnection();
            statement = con.prepareStatement(query);
            if (parameters != null)
            {
                int index = 1;
                for (Object value : parameters)
                {
                    statement.setObject(index++, value);
                }
            }
            if (isReadOnly)
            {
                resultSet = statement.executeQuery();
            } else
            {
                statement.execute();
            }
            return (resultSet == null) ? new ResultData() : new ResultData(resultSet);
        } finally
        {
            close(resultSet);
            close(statement);
            close(con);
        }
    }

    private static void close(AutoCloseable closeable) throws Exception
    {
        if (closeable != null)
        {
            closeable.close();
        }
    }
}
