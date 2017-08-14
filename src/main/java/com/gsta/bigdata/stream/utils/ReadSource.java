package com.gsta.bigdata.stream.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by fanjj on 2017/8/11.
 */
public class ReadSource {
    public static Map<String,ArrayList<String>> readCgiSource(){
        String key = "";
        ArrayList<String> list = new ArrayList<String>();
        Map<String,ArrayList<String>> mapCGI = new HashMap<String,ArrayList<String>>();
        String  lineStr ="";
        try{
            FileInputStream fis = new FileInputStream("conf/cgi.txt");
            InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
            BufferedReader bufReader = new BufferedReader(isr);
            while(true)
            {
                lineStr = bufReader.readLine();
                if(lineStr != null && lineStr.endsWith(":"))
                {
                    if (!key.equals("")){
                        mapCGI.put(key, list);
                        list=new ArrayList<String>();
                    }
                    key = lineStr.split("\\:")[0];
                }else
                if(lineStr != null && !lineStr.endsWith(":")){
                    list.add(lineStr);
                }
                else
                {
                    mapCGI.put(key, list);
                    break;
                }
            }
            bufReader.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return mapCGI;
    }
}
