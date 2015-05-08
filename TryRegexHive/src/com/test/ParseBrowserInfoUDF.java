package com.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


public class ParseBrowserInfoUDF extends UDF {
	
	@Description(name = "testudf",
	value = "_FUNC_(input) - from the input date string "+
	"or separate month and day arguments, returns the sign of the Zodiac.",
	extended = "Example:\n"
	+ " > SELECT _FUNC_(input_string) FROM src;\n")
	
	
	static final String BROWSER_DT = "browserDt";
	static final String BROWSER_NAME = "browserName";
	static Map<String,Map<String,String>> BrowserMap = null;
	//final String FILE_PATH = "/home/chakrabt/hiveCustomJar/hiveBrowserNameDt.txt";
	final String FILE_PATH = "C:\\Users\\562208\\Desktop\\hive\\hiveBrowserNameDt.txt";
	
	public String evaluate(String agent, String type) {
		String result = null;
		try{
			if(BrowserMap == null || BrowserMap.isEmpty()){
				BrowserMap = new HashMap<String, Map<String,String>>();
				loadBrowserMap(BrowserMap);
			}
			if(BROWSER_NAME.equals(type)){
				result = getBrowserName(agent, type);
			}else if(BROWSER_DT.equals(type)){
				result = getBrowserDt(agent, type);
			}
			if(result == null || ("").equals(result) ){
				System.out.println(" agent not found : "+agent+" ,Please ensure that this agent belongs to the property File");
			}
		}catch(Exception ex){
			ex.getMessage();
			ex.printStackTrace();
		}
		System.out.println(result);
		return result;
	}
	
	
	public String getBrowserName(String agent, String type) throws Exception {
		String browserName = "";
		Map<String, String> internalMap = BrowserMap.get(agent);
		if(internalMap != null && !internalMap.isEmpty()){
			browserName = internalMap.get(type);
		}
		return browserName;
	}
	
	public String getBrowserDt(String agent, String type) throws Exception {
		String browserDt = "";
		Map<String, String> internalMap = BrowserMap.get(agent);
		if(internalMap!= null && !internalMap.isEmpty()){
			browserDt = internalMap.get(type);
		}
		return browserDt;
	}
	
	
	public void loadBrowserMap(Map<String,Map<String,String>> inputMap) throws Exception{
		BufferedReader br = null;
		try {
			String sCurrentLine;
		 	br = new BufferedReader(new FileReader(FILE_PATH));
		 	while ((sCurrentLine = br.readLine()) != null) {
				//System.out.println(sCurrentLine);
				String[] strArr = sCurrentLine.split("&!");
				if(strArr != null && strArr.length>2){
					String browserHeader = strArr[0];
					//System.out.println("browserHeader : "+browserHeader);
					Map<String, String> internalMap = new HashMap<String, String>();
					String browserName = strArr[1];
					String browserDetails = strArr[2];
					//System.out.println("browserName : "+browserName);
					//System.out.println("browserDetails : "+browserDetails);
					internalMap.put(BROWSER_NAME, browserName);
					internalMap.put(BROWSER_DT, browserDetails);
					if(browserHeader != null && !("").equals(browserHeader)){
						inputMap.put(browserHeader, internalMap);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args) {
		ParseBrowserInfoUDF parseBrowserInfoUDF = new ParseBrowserInfoUDF();
		parseBrowserInfoUDF.evaluate("Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A403 Safari/8536.25", "browserDt");
	}
}
