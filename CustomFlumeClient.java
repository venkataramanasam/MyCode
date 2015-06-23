package com.tcs.flume.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.tcs.flume.client.SpoolDirectoryFileLister;
import com.tcs.flume.client.CustomClientSpoolStateManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;


public class CustomFlumeClient {

	public static void main(String[] args) throws ParseException {

		PropertyConfigurator.configure("log4j.properties");
		Logger logger = Logger.getLogger(CustomFlumeClient.class);
		Options opts = new Options();

		try
		{
			Option opt = new Option("p","properties", true,"Path of property file");
			opt.setRequired(true);
			opts.addOption(opt);

			opt = new Option("d", "spooldirectory", true, "Path to input directory");
			opt.setRequired(true);
			opts.addOption(opt);

			opt = new Option("h", "help", false, "Display help");
			opt.setRequired(false);
			opts.addOption(opt);

			Parser parser = new GnuParser();
			CommandLine commandLine = parser.parse(opts, args);
			String header = "Note: -h is not mandatory. Please use -p <property file> -d <spool directory> \n\n";
			String footer = "\n";
			if (commandLine.hasOption("h")) {
				new HelpFormatter().printHelp("Usage",header, opts, footer, true);
				return;
			}
		}
		catch(Exception e)
		{
			logger.error("There is an error in specifying arguments for running the program. "+ e);
			String header = "Note: '-h' is not mandatory. Please use -p <property file> -d <spool directory> \n\n";
			String footer = "\n";
			new HelpFormatter().printHelp("Usage",header, opts,footer, true);
			return;
		}


		int fileStatus;
		MyRpcClientFacade client = new MyRpcClientFacade();
		client.init(args[0], args[1]);
		logger.info("Started Client process");
		while(true)
		{
			fileStatus=client.sendDataToFlume();

			if(fileStatus==0)
			{
				logger.info("No files are present. Sleeping for 20 secs .....");
				try {
					Thread.sleep(20000);
				} catch (InterruptedException e) {
				}

			}
			else
			{
				continue;
			}

		}
	}


}


class MyRpcClientFacade   {
	private RpcClient client;

	SpoolDirectoryFileLister filesList = new SpoolDirectoryFileLister() ;
	CustomClientSpoolStateManager filesState=new CustomClientSpoolStateManager("/tmp");
	static List<Event> events = new ArrayList<Event>(100);
	String spoolPath;
	Logger logger = Logger.getLogger(MyRpcClientFacade.class);
	public void init( String propertiesfile, String Spoolpath) {

		PropertyConfigurator.configure("log4j.properties");
		logger.info("Initiating Configuration for Client");
		spoolPath = Spoolpath;

		String host1="dmmlw-r410-105:12345";
		String host2="dmmlw-r410-105:12346";

		Properties props = new Properties();
		props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, "default_loadbalance");
		props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1 h2");
		props.setProperty( RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +"h1",host1 );
		props.setProperty( RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +"h2",host2 );
		props.setProperty( RpcClientConfigurationConstants.CONFIG_HOST_SELECTOR, "round_robin");
		props.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE, "deflate");
		props.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_LEVEL, "9");
		props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, "100");
		this.client = RpcClientFactory.getInstance(props);

	}
	private synchronized void reconnectIfRequired() {
		if (client != null && !client.isActive()) {
			logger.info("Closing the client beacause of appendbatch failure");
			try {
				client.close();
				client = null;
			} catch (Exception ex) {
				logger.warn("Failed to close client: "+ ex);
			}
		}

		if (client == null) {
			logger.info("Creating a new client because of appendbatch failure");
			String host1="dmmlw-r410-105:12345";
			String host2="dmmlw-r410-105:12346";
			Properties props = new Properties();
			props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, "default_loadbalance");
			props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1 h2");
			props.setProperty( RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +"h1",host1 );
			props.setProperty( RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +"h2",host2 );
			props.setProperty( RpcClientConfigurationConstants.CONFIG_HOST_SELECTOR, "round_robin");
			props.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE, "deflate");
			props.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_LEVEL, "9");
			props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, "100");
			try {
				this.client = RpcClientFactory.getInstance(props);
			} catch (Exception e) {
				e.printStackTrace();
				logger.warn("Client creation failed. Source may not have been started yet");
			}
		}
	}
	public int sendDataToFlume() {
		PropertyConfigurator.configure("log4j.properties");

		ArrayList< String > pendingFiles = null;
		try {
			ArrayList< String > files = filesList.getFilesInPath(spoolPath);
			filesState.addProcessingList( files );

			logger.debug("Files received from File lister, now  adding the recieved files to pending list");
			pendingFiles = filesState.getPending();

			if(pendingFiles.isEmpty())
			{
				return 0;
			}

		} catch (Exception e) {
			logger.error( e.toString() );
		}   


		for( String file: pendingFiles )
		{
			try {
				logger.info("Processing the file: " + file);
				filesState.markInProcess( file );			
				long startTime = System.currentTimeMillis();
				File tempFile = filesList.getTempLocalInstance( file );
				if( tempFile == null ) {
					logger.error( "Unable to retrieve contents: " + file );
					logger.error( "Marking file in error state: " + file );
					filesState.markError( file );
					continue;
				}

				Path p = Paths.get(file);
				String file1 = p.getFileName().toString();
				String[] fields = file1.split("_");
				String location = fields[0].split("-")[1];
				String timestamp = fields[1]+fields[2].split("//.")[0];
				SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
				Date date = df.parse(timestamp);
				long epoch = date.getTime();
				Map< String, String > headers = new HashMap< String, String >();
				headers.put("timestamp",Long.toString(epoch));
				headers.put("location", location);
				logger.debug("Appropriate headers are added to the event.");
				GZIPInputStream input=null;
				try {
					input = new GZIPInputStream(new FileInputStream(tempFile));
				} catch (FileNotFoundException e) {
					logger.error("Error while reading Gzipped file: "+e);
					e.printStackTrace();
				} catch (IOException e) {
					logger.error("Error on IO: "+e);
					e.printStackTrace();
				}
				String content;
				Reader decoder = new InputStreamReader(input);
				BufferedReader br = new BufferedReader(decoder);

				int failedlines=0,loaded_lines=0,total_lines=0;
				//List<Event> events = new ArrayList<Event>();

				int batchSize = 100;
				//int y=0;
				while ((content = br.readLine()) != null) 
				{
					events.add(EventBuilder.withBody( content, Charset.forName("UTF-8"), headers ));
					if(events.size()==batchSize){
						try {
							client.appendBatch(events);
						} catch (Throwable x) {
							x.printStackTrace();
							logger.error("Error while attempting to write a batch of events to remote host. Creating the client again!");
							reconnectIfRequired();
							boolean success = true;
							while(success){
								try {
									client.appendBatch(events);
									success = false;
									break;
								} catch (Throwable y) {
									y.printStackTrace();
									logger.error("Error while attempting to write a batch of events to remote host after recreating the client!");
									reconnectIfRequired();
									Thread.sleep(20000);
								}
							}
						}
						events.clear();
					}
					loaded_lines++;		
					total_lines++;
				}
				if(events.size()!=0){
					try {
						client.appendBatch(events);
					} catch (Throwable x) {
						x.printStackTrace();
						logger.error("Error while attempting to write data to remote host at " + "%s:%s. Events will be dropped!");
						reconnectIfRequired();
						boolean success = true;
						while(success){
							try {
								client.appendBatch(events);
								success = false;
								break;
							} catch (Throwable y) {
								y.printStackTrace();
								logger.error("Error while attempting to write data to remote host at " + "%s:%s. after recreating the client!");
								reconnectIfRequired();
								Thread.sleep(20000);
							}
						}
					}
					events.clear();
				}
				logger.info("Sending " + loaded_lines + " events to the flume agent");
				filesState.markFinished( file );
				filesState.removeList(file);
				long endTime   = System.currentTimeMillis();
				long totalTime = startTime - endTime;
				if(filesList.auditGenerator( file, spoolPath, timestamp, total_lines,loaded_lines, failedlines, startTime, endTime,totalTime)==true)
				{
					logger.info("Audit file has been successfully created for the file "+file );
				}
				else
				{
					logger.warn("Errors in generating audit file for the file"+file);
				}
			} catch (Throwable t) {
				logger.error( "While processing: " + file + " - " + t.toString() );
				filesState.markError( file );
				System.exit(0);
			} 
		}

		client.close();
		return 1;

	}
}
