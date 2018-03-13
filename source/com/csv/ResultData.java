package com.csv;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ResultData
{

    private List<String> headers;
    private List<Row> rows;

    ResultData()
    {
        this.headers = new LinkedList<>();
        this.rows = new LinkedList<>();
    }

    ResultData(ResultSet resultSet) throws SQLException
    {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        this.headers = new LinkedList<String>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++)
        {
            this.headers.add(resultSetMetaData.getColumnName(i));
        }

        this.rows = new LinkedList<>();
        while (resultSet.next())
        {
            Row row = new Row();

            for (int i = 1; i <= this.headers.size(); ++i)
            {
                Object value = resultSet.getObject(i);
                row.set(value);
            }

            this.rows.add(row);
        }
    }

    public Iterator<ResultData.Row> getRows()
    {
        return this.rows.iterator();
    }

    public String get(String columnName)
    {
        return get(ResultData.this.getColumnIndex(columnName.toUpperCase()));
    }

    private String get(int columnIndex)
    {
        return (columnIndex < 0 || this.headers.size() < columnIndex || this.rows.size() == 0) ? null : String.valueOf(this.rows.get(0).get(columnIndex));
    }

    private int getColumnIndex(String columnName)
    {
        return headers.indexOf(columnName);
    }

    public class Row
    {
        LinkedList<Object> values = new LinkedList<>();

        private void set(Object value)
        {
            values.add(value);
        }

        public String get(String columnName)
        {
            return get(ResultData.this.getColumnIndex(columnName.toUpperCase()));
        }

        private String get(int columnIndex)
        {
            return (columnIndex < 0 || values.size() < columnIndex) ? null : String.valueOf(values.get(columnIndex));
        }
    }

}
