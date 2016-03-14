/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package importpaycollect;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class DatabaseManager {
    
    private final String JDBC_CONNECTION_URL;
    private final String DB_URL;
    private final String USER;
    private final String PASS;
    private final String JDBC_CONNECTION_URL1;
    private final String DB_URL1;
    private final String USER1;
    private final String PASS1;
    private final Properties prop;
            
    public DatabaseManager(String JDBC_CONNECTION_URL, String DB_URL, String USER, String PASS, String JDBC_CONNECTION_URL1, String DB_URL1, String USER1, String PASS1, Properties prop) 
    {
        this.JDBC_CONNECTION_URL = JDBC_CONNECTION_URL;
        this.DB_URL = DB_URL;
        this.USER = USER;
        this.PASS = PASS;
        this.JDBC_CONNECTION_URL1 = JDBC_CONNECTION_URL1;
        this.DB_URL1 = DB_URL1;
        this.USER1 = USER1;
        this.PASS1 = PASS1;
        this.prop = prop;
    }
    
    private Connection getConnection()
    {
        Connection connection = null;
        
        try 
        {
            
            Class.forName(JDBC_CONNECTION_URL);

        } catch (ClassNotFoundException e) 
        {

            System.out.println(e.getMessage());

        }
        try 
        {

            connection = DriverManager.getConnection(DB_URL, USER,PASS);
            return connection;

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        }
        
        return connection;
    }

    private Connection getConnection2() {
        Connection connection = null;
        
        try 
        {
            
            Class.forName(JDBC_CONNECTION_URL1);

        } catch (ClassNotFoundException e) 
        {

            System.out.println(e.getMessage());

        }
        try 
        {

            connection = DriverManager.getConnection(DB_URL1, USER1,PASS1);
            return connection;

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        }
        
        return connection;
    }

    public void callStoredProcedure(String Storedproc, String ProcedureName) 
    {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        Connection dbConnection = getConnection();
	CallableStatement callableStatement = null;
        try {
            callableStatement = dbConnection.prepareCall(Storedproc);
            callableStatement.executeUpdate();
            successful = 1;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            successful = 0;
        } finally 
        {
 
            if (callableStatement != null) {
                try {
                    callableStatement.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
        
    }
    
    public void loadCSV(String ProcedureName, File filePath, String table, String col_Names, int id, boolean header) {
        
        PreparedStatement preparedStatement = null;
        Connection dbConnection = null;
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        
        String questionmarks = null;
        String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
        String TABLE_REGEX = "\\$\\{table\\}";
        String KEYS_REGEX = "\\$\\{keys\\}";
        String VALUES_REGEX = "\\$\\{values\\}";
            
        try {
            
        
            br = new BufferedReader(new FileReader(filePath));
            String[] headerRow;
                
            if (header == true) {
                String headerRowString = br.readLine();
                headerRow = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            } else {
                headerRow = col_Names.split(",");
            }

            if (null == headerRow) {
                    throw new FileNotFoundException(
                                    "No columns defined in given CSV file." +
                                    "Please check the CSV file format.");
            }
            
            for(int i = 0; i < headerRow.length; i++)
            {
                if(i == 0)
                {
                    questionmarks = "?,";
                } else if(i < headerRow.length - 1)
                {
                    questionmarks = questionmarks + "?,";
                } else
                {
                    questionmarks = questionmarks + "?";
                }
                  
            }

            String query = SQL_INSERT.replaceFirst(TABLE_REGEX, table);
            query = query.replaceFirst(KEYS_REGEX, col_Names);
            query = query.replaceFirst(VALUES_REGEX, questionmarks);
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            preparedStatement = dbConnection.prepareStatement(query);
            
            if(id == 1) 
            {
                dbConnection.createStatement().execute("DELETE FROM " + table);	
            }
            
            while((line = br.readLine()) != null)
            {

                String headerRowString = line;
                String[] rowCount = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                
                int index = 0;
                for(int i = 1; i <= rowCount.length; i++)
                {
                    String insert = rowCount[index];
                    
                    if(insert.contains("\""))
                    {
                       preparedStatement.setString(i, insert.subSequence(1, insert.lastIndexOf("\"")).toString());
                       index++;
                    }else
                    {
                       preparedStatement.setString(i, insert);
                       index++;
                    }
                    
                }
                
                preparedStatement.addBatch();
            }
            
            preparedStatement.executeBatch();
            dbConnection.commit();
            successful = 1;
            
        } catch (FileNotFoundException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } catch (IOException | SQLException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } finally 
        {
            try {
                if (br != null)
                    br.close();
                if (null != preparedStatement)
                    preparedStatement.close();
                if (null != dbConnection)
                    dbConnection.close();
            } catch (IOException | SQLException ex) {
                System.out.println("LoadCSV Failed " + ex);
                successful = 0;
            }
            
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
    
    public void LoadFromAS400(String ProcedureName, String AS400query, String[] AS400Col_Names, String query) {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        Connection dbConnection = getConnection();
        Connection dbConnection2 = getConnection2();
        
        try {
            ArrayList<String> list = new ArrayList<>();
            try (Statement stmt = dbConnection.createStatement()) {
                rs = stmt.executeQuery(AS400query);
                
                while(rs.next())
                {
                    for (String AS400Col_Name : AS400Col_Names) {
                        list.add(rs.getString(AS400Col_Name));
                    }
                }
            }
            
            
            int checkCount = 1;
            dbConnection2.setAutoCommit(false);
            preparedStatement = dbConnection2.prepareStatement(query);
            int i = 0, size = list.size();
            while(i < size){

                if(checkCount <= AS400Col_Names.length){
                    preparedStatement.setString(checkCount, (String) list.get(i));
                    i++;
                    checkCount++;
                } else{
                    preparedStatement.addBatch();
                    checkCount = 1;
                }
                if(i == size){
                    preparedStatement.addBatch();
                }
            }   
            preparedStatement.executeBatch();
            dbConnection2.commit();
            successful = 1;
            
            
        } catch (SQLException ex) {
            System.out.println("LoadFromAS400 Failed " + ex);
            successful = 0;
        } finally
        {
            try {
                if(rs != null)
                    rs.close();
                if(preparedStatement != null)
                    preparedStatement.close();
                if(dbConnection != null)
                    dbConnection.close();
                if(dbConnection2 != null)
                    dbConnection2.close();
            } catch (SQLException ex) {
                System.out.println("LoadFromAS400 Failed " + ex);
                successful = 0;
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
    
    private void updateStatusTable(String ProcedureName, String currentTime, int successful)
    {

        Connection dbConnection = getConnection2();
        Statement statement = null;
        String updateTable = "UPDATE mgex_riskmgnt_scheduled_tasks " + 
                "SET Last_Run_Time = '" + currentTime + "', " + 
                "Success = " + successful + " WHERE Procedures = '" + ProcedureName + "';";
        
        try 
        {
            statement = dbConnection.createStatement();

            statement.execute(updateTable);

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        } finally 
        {

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

        }
    }
    public void truncateTable(String table){
        try {
            try (Connection dbConnection = getConnection2()) {
                dbConnection.createStatement().execute("Truncate " + table);
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        
    }

    public void loadCSVstress(String ProcedureName, File filePath, String table, String col_Names, int id, boolean header) {
        PreparedStatement preparedStatement = null;
        PreparedStatement preparedStatementUpdate = null;
        Connection dbConnection = null;
        Connection dbConnection2 = null;
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        
        String questionmarks = null;
        String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
        String TABLE_REGEX = "\\$\\{table\\}";
        String KEYS_REGEX = "\\$\\{keys\\}";
        String VALUES_REGEX = "\\$\\{values\\}";
        String SQL_Update = "UPDATE mgex_riskmgnt." + table + 
                        " SET Imported_File_Name = '" + filePath.getName() + "' Where " +
                        "Imported_File_Name IS NULL";
        
        try {
            
        
            br = new BufferedReader(new FileReader(filePath));
            String[] headerRow;
                
            if (header == true) {
                String headerRowString = br.readLine();
                headerRow = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            } else {
                headerRow = col_Names.split(",");
            }

            if (null == headerRow) {
                    throw new FileNotFoundException(
                                    "No columns defined in given CSV file." +
                                    "Please check the CSV file format.");
            }
            
            for(int i = 0; i < headerRow.length; i++)
            {
                if(i == 0)
                {
                    questionmarks = "?,";
                } else if(i < headerRow.length - 1)
                {
                    questionmarks = questionmarks + "?,";
                } else
                {
                    questionmarks = questionmarks + "?";
                }
                  
            }

            String query = SQL_INSERT.replaceFirst(TABLE_REGEX, table);
            query = query.replaceFirst(KEYS_REGEX, col_Names);
            query = query.replaceFirst(VALUES_REGEX, questionmarks);
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            preparedStatement = dbConnection.prepareStatement(query);
            
            if(id == 1) 
            {
                dbConnection.createStatement().execute("DELETE FROM " + table);	
            }
            
            while((line = br.readLine()) != null)
            {

                String headerRowString = line;
                String[] rowCount = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                
                int index = 0;
                for(int i = 1; i <= rowCount.length; i++)
                {
                    String insert = rowCount[index];
                    
                    if(insert.contains("\""))
                    {
                       preparedStatement.setString(i, insert.subSequence(1, insert.lastIndexOf("\"")).toString());
                       index++;
                    }else
                    {
                       preparedStatement.setString(i, insert);
                       index++;
                    }
                    
                }
                
                preparedStatement.addBatch();
            }
            
            preparedStatement.executeBatch();
            dbConnection.commit();
            
            dbConnection2 = getConnection();
            preparedStatementUpdate = dbConnection2.prepareStatement(SQL_Update);
            preparedStatementUpdate.executeUpdate();

            successful = 1;
            
        } catch (FileNotFoundException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } catch (IOException | SQLException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } finally 
        {
            try {
                if (br != null)
                    br.close();
                if (null != preparedStatement)
                    preparedStatement.close();
                if (null != dbConnection)
                    dbConnection.close();
                if (null != preparedStatementUpdate)
                    preparedStatementUpdate.close();
                if (null != dbConnection2)
                    dbConnection2.close();
            } catch (IOException | SQLException ex) {
                System.out.println("LoadCSV Failed " + ex);
                successful = 0;
            }
            
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);

    }

    public void loadFlatFile(String ProcedureName, File filePath, String table, String col_Names, int id, boolean header) {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;
        Connection dbConnection = null;
        PreparedStatement stmt = null;
        BufferedReader br = null;
        String query = "INSERT INTO t_data_globexusers (cme, globex, broker_id"
                        + ", mge, firm_id, mge2, clearing_member, mge3, clearing_member2, cust, n, FileDate)"
                        + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        
        try {
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            br = new BufferedReader(new FileReader(filePath));
            stmt = dbConnection.prepareStatement(query);
            String string, date;
            date = br.readLine();
            while(( string = br.readLine()) != null) 
            {
                stmt.setString(1, string.substring(0, 5));
                stmt.setString(2, string.substring(5, 9));
                stmt.setString(3, string.substring(9, 14));
                stmt.setString(4, string.substring(14, 19));
                stmt.setString(5, string.substring(19, 23));
                stmt.setString(6, string.substring(23, 27));
                stmt.setString(7, string.substring(30, 45));
                stmt.setString(8, string.substring(45, 50));
                stmt.setString(9, string.substring(50, 55));
                stmt.setString(10, string.substring(55, 60));
                stmt.setString(11, string.substring(60, 61));
                stmt.setString(12, date.substring(2, 9));
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            dbConnection.commit();
            successful = 1;
            
        } catch (FileNotFoundException | SQLException ex) {
            System.out.println("loadFlatFile Failed " + ex);
            successful = 0;
        } catch (IOException ex) {
            System.out.println("loadFlatFile Failed " + ex);
            successful = 0;
        } finally
        {
            try {
                if(stmt != null)
                    stmt.close();
                if(dbConnection != null)
                    dbConnection.close();
                if(br != null)
                    br.close();
            } catch (SQLException | IOException ex) {
                System.out.println("loadFlatFile Failed " + ex);
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }

    public void LoadFromAS400Trades(String ProcedureName, String AS400query, String AS400_SP, String query) {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        Connection dbConnection = getConnection();
        Connection dbConnection2 = getConnection2();
        String Yr, Mon, Day;
        
        try {
            dbConnection2.setAutoCommit(false);

            try (CallableStatement cStmt = dbConnection.prepareCall(AS400_SP)) {
                cStmt.execute();
                
                try (Statement stmt = dbConnection.createStatement()) {
                    rs = stmt.executeQuery(AS400query);
                    preparedStatement = dbConnection2.prepareStatement(query);
                    while (rs.next())
                    {
                        
                        if(rs.getString("trfrch").trim().equals("S")){
                            
                            preparedStatement.setString(1, rs.getString("frsnum").trim() + "1");
                            
                        }else{
                            
                            preparedStatement.setString(1, rs.getString("frsnum").trim() + "2");
                            
                        }
                        
                        if(rs.getString("trfrch").trim().equals("S")){
                            
                            preparedStatement.setString(4, "SEG");
                            
                        }else{
                            
                            preparedStatement.setString(4, "REG");
                            
                        }
                        
                        preparedStatement.setString(2, rs.getString("tracct").trim());
                        
                        preparedStatement.setString(3, rs.getString("trpbro").trim());
                        
                        Yr = rs.getString("trdate").substring(3, 7).trim();
                        
                        if(rs.getString("trdate").substring(0, 1).length() < 2){
                            
                            Mon = "0" + rs.getString("trdate").substring(0, 1).trim();
                            
                        }else {
                            
                            Mon = rs.getString("trdate").substring(0, 1).trim();
                            
                        }
                        
                        Day = rs.getString("trdate").substring(1, 3).trim();
                        
                        preparedStatement.setString(5, Yr + "-" + Mon + "-" + Day);
                        
                        preparedStatement.setString(6, rs.getString("trtime"));
                        
                        preparedStatement.setString(7, "MGE");
                        
                        preparedStatement.setString(8, "MGE");
                        
                        preparedStatement.setString(9, "M"+rs.getString("trcomm").substring(0, 1).trim()); 
                        
                        if(rs.getString("trstrk").length() > 2){
                            
                            preparedStatement.setString(10, "OOF");
                            
                        } else{
                            
                            preparedStatement.setString(10, "FUT");
                            
                        }
                        
                        if(rs.getString("prmon").trim().length() == 1){
                            
                            preparedStatement.setString(11, rs.getString("pryr4").trim() + "0" + rs.getString("prmon").trim());
                            
                        }else{
                            
                            preparedStatement.setString(11, rs.getString("pryr4").trim() + rs.getString("prmon").trim());
                            
                        }
                        
                        if(rs.getString("trbs").trim().equals("B")){
                            
                            preparedStatement.setString(12, rs.getString("trqty").trim());
                            
                        }else{
                            
                            preparedStatement.setString(12, "-" + rs.getString("trqty").trim());
                            
                        }
                        
                        float trdprice = Float.parseFloat(rs.getString("trpric").trim());
                        
                        trdprice = trdprice / 100000;
                        
                        preparedStatement.setFloat(13, trdprice);
                        
                        preparedStatement.setString(14, rs.getString("trisrc").trim());
                        
                        preparedStatement.setString(15, rs.getString("trelec").trim());
                        
                        preparedStatement.setString(16, rs.getString("trtrid").trim());
                        
                        preparedStatement.addBatch();
                    }
                    
                    preparedStatement.executeBatch();
                    dbConnection2.commit();
                    successful = 1;
                }
            }
        
        }catch(SQLException sqle){
            System.out.println("SQLException : " + sqle);
        }finally
        {
            try {
                if(rs != null)
                    rs.close();
                if(preparedStatement != null)
                    preparedStatement.close();
                if(dbConnection != null)
                    dbConnection.close();
                if(dbConnection2 != null)
                    dbConnection2.close();
            } catch (SQLException ex) {
                System.out.println("LoadFromAS400Trades Failed " + ex);
                successful = 0;
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
   
}
