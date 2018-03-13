package com.csv.util;

import java.io.File;
import java.io.FileNotFoundException;

public class CSVUtil
{
    public static String getTableNameFromCSVFile(String fileName) throws FileNotFoundException
    {
        File file = new File(fileName);

        if(!file.exists())
        {
            throw new FileNotFoundException();
        }

        return file.getName().toUpperCase();
    }
}
