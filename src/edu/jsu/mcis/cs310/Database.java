package edu.jsu.mcis.cs310;

import java.sql.*;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Database {
    
    private final Connection connection;
    
    private final int TERMID_SP22 = 1;
    
    /* CONSTRUCTOR */

    public Database(String username, String password, String address) {
        
        this.connection = openConnection(username, password, address);
        
    }
    
    /* PUBLIC METHODS */

    public String getSectionsAsJSON(int termid, String subjectid, String num) {
        
        String result = null;
        
        //try and catch method
        try {
            /* Preparing Query */
            String query = "SELECT * FROM section WHERE termid = ? AND subjectid = ? AND num = ?";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setInt(1, termid);
            ps.setString(2, subjectid);
            ps.setString(3, num);

            ResultSet rs = ps.executeQuery();

            
            result = getResultSetAsJSON(rs); // Cconverts the result to a string

            rs.close();
        }
        catch (Exception e) { e.printStackTrace(); }

        
        return result;
        
    }
    
    public int register(int studentid, int termid, int crn) {
        
        int result = 0;
        
        try {
            String query = "INSERT INTO registration (studentid, termid, crn) VALUES (?, ?, ?)";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setInt(1, studentid);
            ps.setInt(2, termid);
            ps.setInt(3, crn);

            result = ps.executeUpdate(); 
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }

    public int drop(int studentid, int termid, int crn) {
        
        int result = 0;
        
        try {
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ? AND crn = ?";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setInt(1, studentid);
            ps.setInt(2, termid);
            ps.setInt(3, crn);

            result = ps.executeUpdate();
        } 
        catch (Exception e) { e.printStackTrace(); }

        
        return result;
        
    }
    
    public int withdraw(int studentid, int termid) {
        
        int result = 0;
        
        try {
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ?";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setInt(1, studentid);
            ps.setInt(2, termid);

            result = ps.executeUpdate();
        } 
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public String getScheduleAsJSON(int studentid, int termid) {
        
        String result = null;
        
        JSONArray json = new JSONArray();

        int rowCount = 0; // Keep track of number of rows returned
        int[] crns; // Array to hold CRNs

        try {
            /* Preparing Query */
            String query = "SELECT * FROM registration WHERE studentid = ? AND termid = ?";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.setInt(1, studentid);
            ps.setInt(2, termid);
            
            ResultSet rs = ps.executeQuery();

            /* Get number of rows returned */
            while(rs.next()) {
                rowCount++;
            }

            /* Create array to hold CRNs */
            crns = new int[rowCount];
            
            /* Reset ResultSet */
            rs = ps.executeQuery();
            
            /* Populate array with CRNs */
            for(int i = 0; i < rowCount; i++) {
                rs.next();
                crns[i] = rs.getInt(3);
            }

            for(int i = 0; i < crns.length; i++) {
                /* Search for schedule attributes using CRNs */
                query = "SELECT scheduletypeid, instructor, num, `start`, days, section, `end`, `where`, crn, subjectid FROM section WHERE crn = ?";
                ps = connection.prepareStatement(query);
                ps.setInt(1, crns[i]);

                rs = ps.executeQuery();

                ResultSetMetaData metadata = rs.getMetaData();
                int columnCount = metadata.getColumnCount();

                /* Populating the JSON Array */
                while (rs.next()) {
                    JSONObject obj = new JSONObject();

                    obj.put("studentid", Integer.toString(studentid)); // Gets studentid
                    obj.put("termid", Integer.toString(termid)); // Gets termid

                    for (int j = 1; j <= columnCount; j++) { // Gets all other attributes
                        String key = metadata.getColumnName(j);
                        String value = rs.getString(j);

                        obj.put(key, value);
                    }
                    json.add(obj);
                }
            }

            result = JSONValue.toJSONString(json); // Convert JSONArray to JSON String
        } 
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    public int getStudentId(String username) {
        
        int id = 0;
        
        try {
        
            String query = "SELECT * FROM student WHERE username = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, username);
            
            boolean hasresults = pstmt.execute();
            
            if ( hasresults ) {
                
                ResultSet resultset = pstmt.getResultSet();
                
                if (resultset.next())
                    
                    id = resultset.getInt("id");
                
            }
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return id;
        
    }
    
    public boolean isConnected() {

        boolean result = false;
        
        try {
            
            if ( !(connection == null) )
                
                result = !(connection.isClosed());
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    /* PRIVATE METHODS */

    private Connection openConnection(String u, String p, String a) {
        
        Connection c = null;
        
        if (a.equals("") || u.equals("") || p.equals(""))
            
            System.err.println("*** ERROR: MUST SPECIFY ADDRESS/USERNAME/PASSWORD BEFORE OPENING DATABASE CONNECTION ***");
        
        else {
        
            try {

                String url = "jdbc:mysql://" + a + "/jsu_sp22_v1?autoReconnect=true&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=America/Chicago";
                // System.err.println("Connecting to " + url + " ...");

                c = DriverManager.getConnection(url, u, p);

            }
            catch (Exception e) { e.printStackTrace(); }
        
        }
        
        return c;
        
    }
    
    private String getResultSetAsJSON(ResultSet resultset) {
        
        String result;
        
        /* Create JSON Containers */
        
        JSONArray json = new JSONArray();
        JSONArray keys = new JSONArray();
        
        try {
            
            /* Get Metadata */
        
            ResultSetMetaData metadata = resultset.getMetaData();
            int columnCount = metadata.getColumnCount();
            
             /* Populate the JSON Array */
             while (resultset.next()) {
                JSONObject obj = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String key = metadata.getColumnName(i);
                    String values = resultset.getString(i);

                    keys.add(key);
                    obj.put(key, values);
                }
                json.add(obj);
            }
        
        }
        catch (Exception e) { e.printStackTrace(); }
        
        /* Encode JSON Data and Return */
        
        result = JSONValue.toJSONString(json);
        return result;
        
    }
    
}