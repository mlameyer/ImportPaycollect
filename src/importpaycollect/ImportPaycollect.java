/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package importpaycollect;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class ImportPaycollect {

    private static CurrentTime ct;
    private final static String ProcedureName = "ImportPaycollect";
    private static DatabaseManager dbm;
    private static final String AS400query = "Select * from CLRDAT.CLRMGNT";
    private static final String[] AS400Col_Names = {"MGEXCH","MGFIRM","MGFRCH",
        "MGYBCS","MGYBNC","MGYMRQ","MGTFLF","MGTFLO","MGTFCF","MGTFCO","MGTTCS",
        "MGTTNC","MGTMRQ","MGTMOF","MGTCTI","MGTCTO","MGTBCS","MGTBNC","MGDATE",
        "MGTGAR"};
    private static final String queryPaycollect = "INSERT INTO t_dataclrpaycollect (Exchange, Clearing_Member, "
                + "Reg_Seg, Yesterday_Cash_Balance, Yesterday_non_cash_Balance, "
                + "Yesterday_Margin_Requirement, Futures_Fluctuation, Option_Fluctuation, "
                + "Intraday_Variation, NA, NA2, NA3, Current_Margin_Requirement, "
                + "NA4, End_of_Day_Pay, End_of_Day_Collect, New_Cash_Margin, "
                + "New_Noncash_Margin, Date2, Residual) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String tableDailyCollateral = "t_data_dailycollateral";
    private static final String queryDailycollateral = "INSERT INTO t_data_dailycollateral (Exchange, Clearing_Member, "
                + "Reg_Seg, Yesterday_Cash_Balance, Yesterday_non_cash_Balance, "
                + "Yesterday_Margin_Requirement, Futures_Fluctuation, Option_Fluctuation, "
                + "Intraday_Variation, NA, NA2, NA3, Today_Margin_Requirement, "
                + "NA4, End_of_Day_Pay, End_of_Day_Collect, New_Cash_Margin, "
                + "New_Noncash_Margin, Date2, Residual) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            InputStream inputStream = null;
            Properties prop = new Properties();
            
            String propFileName = "C:\\Program Files\\AutomationControlapp\\ImportPaycollect\\config.properties";
            inputStream = new FileInputStream(propFileName);
            
            prop.load(inputStream);
            
            String JDBC_CONNECTION_URL = prop.getProperty("remote_JDBC_CONNECTION_URL2");
            String DB_URL = prop.getProperty("remote_DB_URL2") + prop.getProperty("remote_DB_CLRDAT");
            String USER = prop.getProperty("remote_DB_USER2");
            String PASS = prop.getProperty("remote_DB_PASS2");
            String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
            String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
            String USER1 = prop.getProperty("local_DB_USER");
            String PASS1 = prop.getProperty("local_DB_PASS");

            ct = new CurrentTime(prop);   
            dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
            
            dbm.LoadFromAS400(ProcedureName, AS400query, AS400Col_Names, queryPaycollect);
            dbm.truncateTable(tableDailyCollateral);
            dbm.LoadFromAS400(ProcedureName, AS400query, AS400Col_Names, queryDailycollateral);
        
        } catch (FileNotFoundException ex) {

        } catch (IOException ex) {

        }
    }
    
}
