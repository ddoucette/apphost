package com.mycompany.HelloWorld;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        int iteration = 0;
        while (true)
        {
            System.out.println("Hello World! (" + iteration++ + ")");
            if ( args.length > 0 )
                System.out.println("ARG[0]: " + args[0]);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
            if (iteration > 10)
                break;
        }
    }
}
