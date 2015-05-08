package com.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {
	
	
	public static String referenceRegex(String text){
		//String regEx = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?";
		//String regEx = ".*(?:(?=Mozilla|Jakarta)(.*)(?=\"Host))";
        //String regEx = "(?:.*(?:accesslog ([\\d\\.]+)).*(?<=\\\").*((?<=\\\").*(?=\\\" \\\"Host)).*)";//"(?:.*(?:accesslog ([\\d\\.]+)).*(?=Mozilla|Jakarta)(.*)(?=\\\" \"Host).*)";//"(?:(?:accesslog ([\\d\\.]+)).*(?=Mozilla|Jakarta)(.*)(?=\\\" \"Host).*)";
		String regEx = "(?:[^,\\s][^\\,]*)";//"(?:[^,\\s][^\\,]*)";//"([^,\\s][^\\,]*[^,\\s]*)";
	  	Pattern pattern = Pattern.compile(regEx);
		Matcher m = pattern.matcher(text);
	    while(m.find()){
	    	for(int i=0; i<=m.groupCount();i++){
	    		String str = m.group(i);
		    	System.out.println("i : "+i+" str :"+str);	
	    	}
	    	
//	    	System.out.println(m.groupCount()); 
	    	//text = text.replace(str, "[*]");
	    }
	  return text;
	}
	
	public static void main(String[] args) {
		//String testStr = "2013-11-05T00:00:11-05:00 eztest-web-73-239 accesslog 10.160.73.218 97.100.54.192, 10.160.73.218 - - [05/Nov/2013:00:00:05 -0500] \"GET /ext/hssl/hssl_chat/invitewindow.html?inviteSwfUrl=http://chat.mcgraw-hill.com/openmeetings/mainInvitePopUp.swf&userId=7243941&sectionId=15524630&sessionId= HTTP/1.1\" 304 -  \"http://ezto.mhecloud.mcgraw-hill.com/hm.tpx\" \"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Windows NT 6.1; Trident/5.0)\" \"Host: ezto-admin.mhecloud.mcgraw-hill.com\" 0 4702";
		String testStr = "SHOW_TEST_COUNT ,2013-10-28,13252699834180562,15525373,600150837,12230479,1";
		String retrnStr = referenceRegex(testStr);
		System.out.println(" testStr : "+testStr);
		//System.out.println(" retrnStr : "+retrnStr);
	}
}
