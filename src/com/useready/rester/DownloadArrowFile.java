package com.useready.rester;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import com.simba.support.ILogger;
import com.simba.support.LogUtilities;

public class DownloadArrowFile {

	public DownloadArrowFile(String fileurl,ILogger iLogger) {
	
    String savePath = "D:\\JDBCDriver29Aug\\SimbaJDbc-BofaConnector.29Aug\\sample.arrow"; // Save in the current working directory

    try {
        URL url = new URL(fileurl);
        URLConnection connection = url.openConnection();
        InputStream inputStream = connection.getInputStream();
        FileOutputStream outputStream = new FileOutputStream(savePath);

        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }

        outputStream.close();
        inputStream.close();
        LogUtilities.logDebug("File downloaded successfully- ", iLogger);
     
		try{
			   File file = new File(savePath);
			 BufferAllocator rootAllocator = new RootAllocator();
			    FileInputStream fileInputStream = new FileInputStream(file);
			    ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator);
			LogUtilities.logDebug("Record batches in file: :::::: "+ reader.getRecordBlocks().size(), iLogger);
		    for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
		        reader.loadRecordBatch(arrowBlock);
		        VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
		        LogUtilities.logDebug("Data in file: :::::: "+ vectorSchemaRootRecover.contentToTSVString(), iLogger);
		       // System.out.print(vectorSchemaRootRecover.contentToTSVString());
		    }
		} catch (Exception e) {
			LogUtilities.logDebug("Exception :::::: "+e.getMessage() , iLogger);
		    e.printStackTrace();
		    e.getCause();
		}
		
    } catch (IOException e) {
        e.printStackTrace();
        LogUtilities.logDebug("File download failed-- "+e.getMessage(), iLogger);
        System.err.println("File download failed.");
    }
	}

}
