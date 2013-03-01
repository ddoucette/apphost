
package com.sdde.DukascopyController;

import java.sql.*;
import java.io.*;
import java.text.SimpleDateFormat;

public class Llog {

    private static final int LOG_LEVEL_ERROR = 0;
    private static final int LOG_LEVEL_INFO = 1;
    private static final int LOG_LEVEL_DEBUG = 2;
    private static final int LOG_LEVEL_DEFAULT = LOG_LEVEL_DEBUG;

    private SimpleDateFormat df;
    private String mod_name;
    private int log_level = LOG_LEVEL_DEFAULT;
    private PrintStream log_stream = System.out;

    public Llog (String mod_name)
    {
        this.mod_name = mod_name;
        this.df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS");
    }

    public void set_level(int level)
    {
        assert level >= LOG_LEVEL_ERROR && level <= LOG_LEVEL_DEBUG;
        this.log_level = level;
    }

    public void debug(String msg)
    {
        this.log(LOG_LEVEL_DEBUG, msg);
    }

    public void info(String msg)
    {
        this.log(LOG_LEVEL_INFO, msg);
    }

    public void error(String msg)
    {
        this.log(LOG_LEVEL_ERROR, msg);
    }

    private void log ( int level, String msg )
    {
        if ( level > this.log_level )
            return;

        String lvl_str = "UNKNOWN";
        switch (level)
        {
            case LOG_LEVEL_ERROR:
                lvl_str = "ERROR";
                break;

            case LOG_LEVEL_INFO:
                lvl_str = "INFO";
                break;

            case LOG_LEVEL_DEBUG:
                lvl_str = "DEBUG";
                break;

            default:
                assert false;
        }

        Date d = new Date(System.currentTimeMillis());
        this.log_stream.format("%s %7s (%s) %s\n",
                               df.format(d),
                               lvl_str,
                               this,
                               msg);
    }

    public String toString()
    {
        return this.mod_name;
    }
}
