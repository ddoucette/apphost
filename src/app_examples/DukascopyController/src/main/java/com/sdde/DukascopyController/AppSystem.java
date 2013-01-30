
package com.sdde.DukascopyController;


public class AppSystem {

    private static String g_user_name;
    private static String g_application_name;

    private AppSystem() {}

    public static void Init (String user_name, String application_name)
    {
        assert g_user_name == null;
        assert g_application_name == null;

        g_user_name = user_name;
        g_application_name = application_name;
    }

    public static String GetUserName ()
    {
        return g_user_name;
    }

    public static String GetApplicationName ()
    {
        return g_application_name;
    }
}
