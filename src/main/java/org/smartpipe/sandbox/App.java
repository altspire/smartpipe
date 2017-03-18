package org.smartpipe.sandbox;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.smartpipe.lib.MongoDbHelper;
import org.smartpipe.lib.WatchDir;

import com.mongodb.*;
/**
 * Hello world!
 *
 */
public class App 
{
    static void usage() {
        System.err.println("usage: java WatchDir [-r] dir");
        System.exit(-1);
    }
    
    public static void main( String[] args ) throws IOException, InterruptedException
    {
        if (args.length == 0 || args.length > 2)
            usage();
        boolean recursive = false;
        int dirArg = 0;
        if (args[0].equals("-r")) {
            if (args.length < 2)
                usage();
            recursive = true;
            dirArg++;
        }
 
        // register directory and process its events
        Path dir = Paths.get(args[dirArg]);
        new WatchDir(dir, recursive).processEvents();
    }  

}
