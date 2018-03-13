package com.csv;

public interface CSVConstants
{
    String IMPORT_QUERY = "CREATE TABLE %TABLE_NAME% AS SELECT * FROM CSVREAD('%FILE_PATH%')";

    String DROP_QUERY = "DROP TABLE %TABLE_NAME%";

    String FILE_PATH = "%FILE_PATH%";

    String TABLE_NAME = "%TABLE_NAME%";

}
