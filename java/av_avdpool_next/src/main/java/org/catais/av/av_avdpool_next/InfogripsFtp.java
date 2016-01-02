/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.catais.av.av_avdpool_next;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.net.ftp.FTP;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.net.ftp.FTPClient;

/**
 *
 * @author stefan
 */
public class InfogripsFtp {
        static final Logger logger = LogManager.getLogger(InfogripsFtp.class.getName());
        
        String ftphost = null;
        String ftpusr = null;
        String ftppwd =  null;
        String ftpWorkingDir = null;
        String ftpDownloadDir = null;
        
        ArrayList<String> files = new ArrayList<>();
        
        
        public InfogripsFtp(HashMap<String, String> params) { 
            logger.debug("download");
            
            ftphost = params.get("ftphost");
            ftpusr = params.get("ftpusr");
            ftppwd = params.get("ftppwd");
            ftpWorkingDir = params.get("ftpWorkingDir");
            ftpDownloadDir = params.get("ftpDownloadDir");        
        }

        // TODO: proper exception handling
        public ArrayList download() throws IOException {
            FTPClient ftpClient = new FTPClient();
            ftpClient.enterLocalPassiveMode();


            ftpClient.connect(ftphost);
           
            if (!ftpClient.login(ftpusr, ftppwd)) {
                ftpClient.disconnect();
                throw new IOException("Could not login to ftp server.");
            }
            
            if (!ftpClient.setFileType(FTP.BINARY_FILE_TYPE)) {
                ftpClient.disconnect();                
                throw new IOException("Could not set binary file type."); 
            }            
                        
                  
            String[] ftpFileList = ftpClient.listNames(ftpWorkingDir);
            for (String ftpFileName : ftpFileList) {
                logger.debug(ftpFileName);
                
                // TODO: wie funktioniert das genau? wie/wann wird exception geworfen?
                // Ist output.close() nicht mehr nötig?
                String downloadedFileName = ftpDownloadDir + File.separatorChar + ftpFileName;
                try (OutputStream output = new FileOutputStream(downloadedFileName)) {
                    ftpClient.retrieveFile(ftpWorkingDir + File.separatorChar + ftpFileName, output);
                    files.add(downloadedFileName);                    
                }
                break;
            }
            
            ftpClient.disconnect();
            
            return files;
        }
}
