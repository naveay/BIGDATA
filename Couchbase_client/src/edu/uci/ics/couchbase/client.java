package edu.uci.ics.couchbase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
class Stats
{
	private long time;
	private int count;
	private String content;
	public Stats(long time, int count, String content)
	{
		this.time=time;
		this.count=count;
		this.content=content;
	}
	public long getTime()
	{
		return time;
	}
	public int getCount()
	{
		return count;
	}
	public String getContent()
	{
		return content;
	}
}
public class client {
	static URIBuilder query=null;
	static HttpGet httpGet=null;
	static DefaultHttpClient httpclient=null;
	private static File[] getFiles(String dirPath)
	{
		File folder=new File(dirPath);
		File[] listofFiles=folder.listFiles();
		Arrays.sort(listofFiles);
		ArrayList<File> arrayQueries=new ArrayList<File>();
		File[] listofQueries;
		for(File f: listofFiles)
		{
			String fileName=f.getName();
			if(fileName.startsWith("q")&&!fileName.endsWith("~"))
			{
				arrayQueries.add(f);
				System.out.println(fileName);
			}
		}
		listofQueries=new File[arrayQueries.size()];
		for(int i=0;i<arrayQueries.size();i++)
		{
			listofQueries[i]=arrayQueries.get(i);
		}
		return listofQueries;
	}
	private static int getResultsSize(String content)
	{
		int count=0;
		try{
			JsonFactory jsonFactory=new JsonFactory();
			JsonParser resultParser=jsonFactory.createParser(content);
			resultParser.nextToken();
			while(resultParser.getCurrentName()==null||!resultParser.getCurrentName().equals("info"))
			{
				resultParser.nextToken();
			}
			while(resultParser.getCurrentName()==null||!resultParser.getCurrentName().equals("message"))
			{
				resultParser.nextToken();
			}
			resultParser.nextToken();
			count=Integer.parseInt(resultParser.getText());
		}
		catch(JsonParseException e)
		{
			e.printStackTrace();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		return count;
	}
	private static Stats runReadOnlyQuery(String qTxt) throws Exception
	{
		query.setParameter("q",qTxt);
		URI uri=query.build();
		System.out.println(uri.toString());
		httpGet.setURI(uri);
		long s=System.currentTimeMillis();
		HttpResponse response=httpclient.execute(httpGet);
		HttpEntity entity=response.getEntity();
		String content=EntityUtils.toString(entity);
		EntityUtils.consume(entity);
		long e=System.currentTimeMillis();
		long clientTime=(e-s);
		int resSize=getResultsSize(content);
		//System.out.println(content);
		return new Stats(clientTime,resSize,content);
	}
	private static String generateAbsolutePathString(String pathToSymLinkFile, String symLinkPath)
	{
		if(!symLinkPath.startsWith("/"))
		{
			String[] pathToSymLinkArray=pathToSymLinkFile.split("/");
			String[] symLinkPathArray = symLinkPath.split("/");
			int absPathPrefixIndex=pathToSymLinkArray.length-1;
			int absPathSufIndex=0;
			for(String s:symLinkPathArray)
			{
				if (s.equals("..")) {
					absPathPrefixIndex--;
					} else if (!s.equals(".")) {
					break;
					}
					absPathSufIndex++;
			}
			String absPath=new String("");
			for(int i=0;i<absPathPrefixIndex;i++)
			{
				absPath+=pathToSymLinkArray[i]+"/";
			}
			for(int i=absPathSufIndex;i < (symLinkPathArray.length - 1); i++)
			{
				absPath += symLinkPathArray[i] + "/";
			}
			absPath += symLinkPathArray[symLinkPathArray.length - 1];
			return absPath;
		}
		else
		{
			return symLinkPath;
		}
	}

	private static String getFileText(File f)throws Exception
	{
		Path fPath=f.toPath();
		if(Files.isSymbolicLink(fPath))
		{
			String abspath=generateAbsolutePathString(f.getAbsolutePath(),Files.readSymbolicLink(fPath).toString());
			f=new File(abspath);
		}
		BufferedReader in = new BufferedReader(new FileReader(f));
        StringBuffer sb = new StringBuffer();
        String str;
        while ((str = in.readLine()) != null) {
            sb.append(str).append("\n");
        }
        in.close();
        return sb.toString();
	}
	private static String parseResults(String content)
	{
		StringBuffer sb = new StringBuffer();
        try {
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser resultParser = jsonFactory.createParser(content);
            while (resultParser.nextToken() == JsonToken.START_OBJECT) {
                while (resultParser.nextToken() != JsonToken.END_OBJECT) {
                    String key = resultParser.getCurrentName();
                    if (key.equals("resultset")) {
                        // Start of results array
                        resultParser.nextToken();
                        while (resultParser.nextToken() != JsonToken.END_ARRAY) {
                            String record = resultParser.getValueAsString();
                            sb.append(record);
                        }
                    } else {
                        String summary = resultParser.getValueAsString();
                        if (key.equals("info")) {
                            sb.append("Exception - Could not find results key in the JSON Object\n");
                            sb.append(summary);
                            return (sb.toString());
                        }
                    }
                }
            }
            
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (sb.toString());
	}
	public static void main(String args[])
	{
		
		String qDir="/home/alex/BIG_DATA/couchbase_query";
		File[] qfile=getFiles(qDir);
		String[] qTexts=null;
		String statFile = "/home/alex/BIG_DATA/couchbase_stat/stat";
		String resultsDumpFile ="/home/alex/BIG_DATA/couchbase_stat/result"; 
		int iteration=20;
		try{
			qTexts=new String[qfile.length];
			for(int i=0;i<qfile.length;i++)
			{
				qTexts[i]=getFileText(qfile[i]);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		PrintWriter statsPw=null;
		PrintWriter resultsPw=null;
		try
		{
			query=new URIBuilder("http://getafix-macmini.ics.uci.edu:8093/query");
			httpclient = new DefaultHttpClient();
            httpGet = new HttpGet();
            statsPw = new PrintWriter(statFile);
            statsPw.println("Iteration\tQuery-File\tTime(ms)\tResults-count");
            if(resultsDumpFile != null && !resultsDumpFile.equals("null")){
                resultsPw = new PrintWriter(resultsDumpFile);
            }
        } catch (Exception e) {
            System.err.println("Problem in setting request sending utils and/or stats writer");
            e.printStackTrace();
        }
		try
		{
			for(int i=0;i<iteration;i++)
			{
				int ix=0;
				System.out.println("Running in the iteration "+i);
				for(String nxq:qTexts)
				{
					String queryFileName=qfile[ix].getName();
					Stats stat=runReadOnlyQuery(nxq);
					if(stat.getCount()<0)
					{
						System.err.println(qfile[ix].getName()+" in iterattion "+i+" returned invalid results.\n");
                        System.err.println(stat.getContent());
					}
					statsPw.println(i+"\t"+queryFileName+"\t"+stat.getTime()+"\t"+stat.getCount());
					System.out.println(i+"\t"+queryFileName+"\t"+stat.getTime()+"\t"+stat.getCount());
					/*
					if(resultsPw != null && (stat.getCount()> -1)){
                    	String parsedResults = parseResults(stat.getContent());
                        resultsPw.println("Results for "+queryFileName+" in iteration "+i);
                        resultsPw.println(parsedResults);
                        resultsPw.flush();
                    }*/
					String parsedResults = stat.getContent();
                    resultsPw.println("Results for "+queryFileName+" in iteration "+i);
                    resultsPw.println(parsedResults);
                    resultsPw.flush();
    				ix++;
				}
			}
		}
		catch (Exception e) {
            System.err.println("Problem in query execution, stats dumping, or retrieving correct results files.");
            e.printStackTrace();
        } finally{
            if(statsPw != null) { statsPw.close(); resultsPw.close();}
            httpclient.getConnectionManager().shutdown();
            System.out.println("\nAsterix Client finished !");
            
        }
		return;
	}
}
