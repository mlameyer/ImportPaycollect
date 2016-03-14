/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package importpaycollect;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class CurrentTime {
    
    private final Properties prop;
    
    public CurrentTime(Properties prop)
    {
        this.prop = prop;
    }
    
    public String getCurrentTime()
    {
        String currentTime;
        LocalDateTime localNow = LocalDateTime.now();
        currentTime = localNow.toString();
        
        return currentTime;
    }

    public long getTimeDelay(String value) 
    {
        int hour = Integer.parseInt(value.substring(0, value.indexOf(":")));
        int min = Integer.parseInt(value.substring(value.indexOf(":") + 1, value.lastIndexOf(":")));
        int sec = Integer.parseInt(value.substring(value.lastIndexOf(":") + 1));

        LocalDateTime localNow = LocalDateTime.now();
        ZoneId currentZone = ZoneId.of("America/Chicago");
        ZonedDateTime zonedNow = ZonedDateTime.of(localNow, currentZone);
        ZonedDateTime zonedNext ;
        zonedNext = zonedNow.withHour(hour).withMinute(min).withSecond(sec);
        
        if(zonedNow.compareTo(zonedNext) > 0)
        {
            zonedNext = zonedNext.plusDays(1);
        }
        
        Duration duration = Duration.between(zonedNow, zonedNext);
        
        return duration.getSeconds();
    }
    
    public int getDayofWeek()
    {
        LocalDateTime localNow = LocalDateTime.now();
        ZoneId currentZone = ZoneId.of("America/Chicago");
        ZonedDateTime zonedNow = ZonedDateTime.of(localNow, currentZone);
        
        int result = 0;
        
        switch (zonedNow.getDayOfWeek())
        {
            case MONDAY: result = 0;
                        break;
            case TUESDAY: result = 1;
                        break;
            case WEDNESDAY: result = 2;
                        break;
            case THURSDAY: result = 3;
                        break;
            case FRIDAY: result = 4;
                        break;
            case SATURDAY: result = 5;
                        break;
            case SUNDAY: result = 6;
                        break;
        }
        
        return result;
    }

    public boolean getMKOpen() {
        
        boolean MKOpen = false;
        int a = 0; //open 1
        int b = 0; //open 2
        int c = 0; //close 1
        int d = 0; //close 2
        
        String open1 = prop.getProperty("MktOpen");
        String open2 = prop.getProperty("MktOpen1");
        String close1 = prop.getProperty("MktClose");
        String close2 = prop.getProperty("MktClose1");
        
        String currentTime;
        LocalTime localNow = LocalTime.now();
        currentTime = localNow.toString();
        
        int hour = Integer.parseInt(open1.substring(0, open1.indexOf(":")));
        int min = Integer.parseInt(open1.substring(open1.indexOf(":") + 1, open1.lastIndexOf(":")));
        int sec = Integer.parseInt(open1.substring(open1.lastIndexOf(":") + 1));
        LocalTime com = LocalTime.of(hour, min, sec);
        a = currentTime.compareTo(com.toString());
        
        hour = Integer.parseInt(open2.substring(0, open2.indexOf(":")));
        min = Integer.parseInt(open2.substring(open2.indexOf(":") + 1, open2.lastIndexOf(":")));
        sec = Integer.parseInt(open2.substring(open2.lastIndexOf(":") + 1));
        com = LocalTime.of(hour, min, sec);
        b = currentTime.compareTo(com.toString());
        
        hour = Integer.parseInt(close1.substring(0, close1.indexOf(":")));
        min = Integer.parseInt(close1.substring(close1.indexOf(":") + 1, close1.lastIndexOf(":")));
        sec = Integer.parseInt(close1.substring(close1.lastIndexOf(":") + 1));
        com = LocalTime.of(hour, min, sec);
        c = currentTime.compareTo(com.toString());
        
        hour = Integer.parseInt(close2.substring(0, close2.indexOf(":")));
        min = Integer.parseInt(close2.substring(close2.indexOf(":") + 1, close2.lastIndexOf(":")));
        sec = Integer.parseInt(close2.substring(close2.lastIndexOf(":") + 1));
        com = LocalTime.of(hour, min, sec);
        d = currentTime.compareTo(com.toString());
        
        //Open 1
        if (a > 0 && b > 0 && c > 0 && d > 0) // + + + +
        {
            MKOpen = true;
        }
        //Open 1 past midnight
        if (a < 0 && b < 0 && c < 0 && d < 0) // - - - -
        {
            MKOpen = true;
        }
        //Close 1
        if (a < 0 && b < 0 && c > 0 && d < 0) // - - + -
        {
            MKOpen = false;
        }
        //Open 2
        if (a < 0 && b > 0 && c > 0 && d < 0) // - + + -
        {
            MKOpen = true;
        }
        //Close 2
        if (a < 0 && b > 0 && c > 0 && d > 0) // - + + +
        {
            MKOpen = false;
        }
        
        return MKOpen;
    }

    String getPreviousDate() {
        String PreviousDay;
        
        LocalDate ld = LocalDate.now(ZoneId.of("America/Chicago"));
        PreviousDay = ld.minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
        
        return PreviousDay;
    }
    
    String getCurrentDate() {
        String CurrentDay;
        
        LocalDate ld = LocalDate.now(ZoneId.of("America/Chicago"));
        CurrentDay = ld.format(DateTimeFormatter.BASIC_ISO_DATE);
        
        return CurrentDay;
    }
    
}
