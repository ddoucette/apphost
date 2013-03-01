package com.mycompany.HelloWorld;

import com.sdde.DukascopyController.*;


/**
 * Hello world!
 *
 */
public class App extends Thread
{
    private NumericEvent evt1;
    private NumericEvent evt2;
    private NumericEvent evt3;

    public App (String user_name, String application_name)
    {
        AppSystem.Init(user_name, application_name);

        evt1 = new NumericEvent("evt1");
        evt2 = new NumericEvent("evt2");
        evt3 = new NumericEvent("evt3");
    }

    public void run ()
    {
        int iteration = 0;

        while (true)
        {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }

            evt1.send(1.0 + iteration);
            evt2.send(30 + iteration);
            evt3.send(67.965 + iteration);
            iteration++;

            /* Do something to generate a crash. */
            if ( iteration == 20 )
                evt3 = null;
        }
    }

    public static void main( String[] args )
    {
        System.out.println("Hello World! " + args.length + " arguments.");
        System.out.println("arg[0]: " + args[0]);
        System.out.println("Working Directory = " +
                      System.getProperty("user.dir"));
        App app = new App(args[0], args[1]);
        app.start();
    }
}
