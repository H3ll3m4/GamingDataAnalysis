package com.litquidity.sql;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.util.ArrayList;
//import com.its.util.SQLHelper;
import java.sql.DatabaseMetaData;

public class Select {

    /**
     * Connect to the test.db database
     * @return the Connection object
     */
    private Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:C:\\Documents\\ML\\DataScrum Trading Hackathon\\altdata-hackathon\\data\\Hachathon.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
 
    public ResultSet showTables() {
    	ResultSet rs = null;
    	
    	//https://examples.javacodegeeks.com/core-java/sql/list-all-database-table-names/
        try {
        	Connection conn = this.connect();
        	  // Get the database metadata     	 
        	  DatabaseMetaData metadata = conn.getMetaData();
        	  // Specify the type of object; in this case we want tables       	 
        	  //String[] types = {"TABLE"};
        	  //rs = metadata.getTables(null, null, "%", types);
        	  //https://stackoverflow.com/questions/2780284/how-to-get-all-table-names-from-a-database
        	  rs = metadata.getTables(null, null, "%", null);
        	  while (rs.next()) {     	 
        	    String tableName = rs.getString(3);
        	    String tableCatalog = rs.getString(1);
        	    String tableSchema = rs.getString(2);
        	    System.out.println("Table : " + tableName + "nCatalog : " + tableCatalog + "nSchema : " + tableSchema);
        	  }
        	    } catch (SQLException e) {
        	  System.out.println("Could not get database metadata " + e.getMessage());
        	    }
        	    
    	return rs;
    }
    
 
    /**
     * select all rows in the genres table
     */
    public void displayGenres(){
        String sql = "SELECT id, genre_name FROM genres";
        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" + 
                                   rs.getString("genre_name") );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void displayCompanies(){
        String sql = "SELECT id, ticker, company_name FROM companies";
        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" +
                					rs.getString("ticker") +  "\t" +
                                   rs.getString("company_name") );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
   
    public int getCompanyId(String companyName){
        String sql = "SELECT id, ticker, company_name FROM companies WHERE company_name = "+companyName;
        int id = -1 ; 
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            id = rs.getInt("id");
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" +
                					rs.getString("ticker") +  "\t" +
                                   rs.getString("company_name") );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return id;
    }
    
    public void displayRevenues(int companyId){
        String sql = "SELECT id, company_id, date, total_revenue, cost_of_revenue FROM revenues WHERE company_id = "+ companyId;
        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" +
                					rs.getInt("company_id") +  "\t" +
                                    rs.getDate("date")+  "\t" +
                                    rs.getFloat("total_revenue")+  "\t" +
                                   rs.getFloat("cost_of_revenue") );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    
    public void displayStockPrice(int companyId){
        String sql = "SELECT id, company_id, date, open, close FROM stock_prices WHERE company_id = "+companyId;
        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" +
                					rs.getInt("company_id") +  "\t" +
                                    rs.getTimestamp("date")+  "\t" +
                                    rs.getFloat("open")+  "\t" +
                                   rs.getFloat("close") );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void displaySales(String publisherName){
    	int id = -1;
    	String sql = "SELECT id FROM publishers WHERE publisher_name =" + publisherName ;
        try (Connection conn = this.connect();
                Statement stmt  = conn.createStatement();
                ResultSet rs    = stmt.executeQuery(sql)){
               id = rs.getInt("id");
               // loop through the result set
               while (rs.next()) {
                   System.out.println(rs.getInt("id") );
               }
           } catch (SQLException e) {
               System.out.println(e.getMessage());
           }
    	
        String sql2 = "SELECT id, game_name, genre, publisher, year, global_sales FROM video_game_sales WHERE publisher_id = "+id;
        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql2)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" +
                					rs.getString("game_name") +  "\t" +
                                    rs.getString("genre") +  "\t" +
                                    rs.getString("publisher") +  "\t" +
                                   rs.getInt("year") +  "\t" +
                                   rs.getFloat("global_sales"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void displayWebTraffic(int companyId){
        String sql = "SELECT id, company_id, date, total_visits, total_page_views FROM websites WHERE company_id = "+ companyId;
        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" +
                					rs.getInt("company_id") +  "\t" +
                                    rs.getDate("date")+  "\t" +
                                    rs.getFloat("total_visits")+  "\t" +
                                   rs.getFloat("total_page_views") );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Select app = new Select();
        //Connection c = app.connect();
        //System.out.println(c.toString());
        app.showTables();
        String name = "Blizzard";
        int id = app.getCompanyId(name);
        app.displayRevenues(id);
        app.displayStockPrice(id);
        app.displayWebTraffic(id);
        app.displaySales(name);
    }

}
