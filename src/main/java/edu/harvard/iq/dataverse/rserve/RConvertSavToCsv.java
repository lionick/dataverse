package edu.harvard.iq.dataverse.rserve;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.ejb.Stateless;
import javax.inject.Named;

import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;

@Stateless
@Named
public class RConvertSavToCsv {
    private static Logger logger = Logger.getLogger(RConvertSavToCsv.class.getPackage().getName());

    private static String RSERVE_HOST = null;
    private static String RSERVE_USER = null;
    private static String RSERVE_PWD = null;    
    private static int    RSERVE_PORT = -1;
    public static String RSERVE_TMP_DIR=null;
    
    static {
    
        RSERVE_TMP_DIR = System.getProperty("dataverse.rserve.tempdir");
        
        if (RSERVE_TMP_DIR == null){
            RSERVE_TMP_DIR = "/tmp/";            
        }
        
        RSERVE_HOST = System.getProperty("dataverse.rserve.host");
        if (RSERVE_HOST == null){
            RSERVE_HOST= "localhost";
        }
        
        RSERVE_USER = System.getProperty("dataverse.rserve.user");
        if (RSERVE_USER == null){
            RSERVE_USER= "rserve";
        }
        
        RSERVE_PWD = System.getProperty("dataverse.rserve.password");
        if (RSERVE_PWD == null){
            RSERVE_PWD= "rserve";
        }
        

        if (System.getProperty("dataverse.rserve.port") == null ){
            RSERVE_PORT= 6311;
        } else {
            RSERVE_PORT = Integer.parseInt(System.getProperty("dataverse.rserve.port"));
        }

    }

    private RConnection setupConnection() throws REXPMismatchException, RserveException {
        // Set up an Rserve connection
        logger.fine("RSERVE_USER="+RSERVE_USER+"[default=rserve]");
        logger.fine("RSERVE_PASSWORD="+RSERVE_PWD+"[default=rserve]");
        logger.fine("RSERVE_PORT="+RSERVE_PORT+"[default=6311]");
        logger.fine("RSERVE_HOST="+RSERVE_HOST);
        RConnection connection = new RConnection(RSERVE_HOST, RSERVE_PORT);
        connection.login(RSERVE_USER, RSERVE_PWD);
        logger.fine(">" + connection.eval("R.version$version.string").asString() + "<");
        // check working directories
        // This needs to be done *before* we try to create any files
        // there!
        //setupWorkingDirectory(connection);
        return connection;
    }

    public void convertFile(String folderSystemPath,String fileName){
        Map<String, String> result = new HashMap<>();
        try {
        RConnection connection = setupConnection();
        connection.eval("library(foreign)");
        connection.eval("setwd('" + folderSystemPath + File.separator +"')");
        connection.eval("write.table(read.spss('" + fileName + ".orig'), file='/tmp/" + fileName + ".csv', quote = FALSE, sep = ',',row.names=FALSE)");
        
        connection.close();
        }
        catch ( RserveException | REXPMismatchException e) {
            logger.severe(e.getMessage());
            result.put("RexecError", "true");
        }
    }
    
    public void setupWorkingDirectory(RConnection connection) {
        
        try {
            // check the temp directory; try to create it if it doesn't exist:

            String checkWrkDir = "if (!file_test('-d', '" + RSERVE_TMP_DIR + "')) {dir.create('" + RSERVE_TMP_DIR + "', showWarnings = FALSE, recursive = TRUE);}";

            logger.fine("w permission=" + checkWrkDir);
            connection.voidEval(checkWrkDir);

        } catch (RserveException rse) {
            rse.printStackTrace();
        }
    }
}