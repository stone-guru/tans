package org.axesoft.jaxos.base;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormater {
    private SimpleDateFormat format = new SimpleDateFormat("MM-dd hh:mm:ss");
    public String format(Date d){
        return format.format(d);
    }
}
