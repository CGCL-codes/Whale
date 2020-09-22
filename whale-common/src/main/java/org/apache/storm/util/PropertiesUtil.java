package org.apache.storm.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by 79875 on 2017/1/11.
 */
public class PropertiesUtil {

    public static Properties pro;

    public static void init(String fileName) {
        pro = new Properties();
        try {
            InputStream in = Object.class.getResourceAsStream(fileName);
            pro.load(in);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 绝对路径初始化
     * @param absolutePath
     */
    public static void initAbsolutePath(String absolutePath) {
        pro = new Properties();
        try {
            InputStream in = new FileInputStream(absolutePath);
            pro.load(in);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getProperties(String name){
        return pro.getProperty(name);
    }

    public static void setProperties(String name ,String value){
        pro.setProperty(name, value);
    }
}
