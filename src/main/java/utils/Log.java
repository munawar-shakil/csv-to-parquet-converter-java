package utils;

import java.util.Date;

public class Log {
    public static Log getLog(Class _className) {
        return new Log(_className);
    }

    private String className = "";

    Log (Class _className) {
        className = _className.getName() + ": ";
    }

    public void warn(String message) {
        System.out.println("WARNING: " + className + new Date().toString()+" " + message);
    }

    public void info(String message) {
        System.out.println("Info: " + className + new Date().toString()+" " + message);
    }
}
