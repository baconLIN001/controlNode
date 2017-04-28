package com.bacon.controlNode.other;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bacon on 2017/4/20.
 */
public class DataBaseTest {
    static String url = "jdbc:mysql://localhost:3306/score";
    static String username = "root";
    static String password = "1234";
    static String tableName = "user_score";

    public static void main(String[] args) throws SQLException {
//        getTableAllData(DbConstant.MYSQL,url,username,password,tableName);
        List<String> tables = getDbTables(DbConstant.MYSQL,url,username,password);

        List<String> fields = new ArrayList<>();
        fields.add("id");
        fields.add("art");
        fields.add("math");
        getTablePartialData(DbConstant.MYSQL,url,username,password,tableName,fields);
    }
    private static Connection getDbConnection(String dbType, String url, String username, String password){
        Connection connection = null;
        try{
            if (dbType.equals(DbConstant.MYSQL)) {
                Class.forName(DbConstant.mysqlDriver);
            }
            else if (dbType.equals(DbConstant.ORACLE)){
                Class.forName(DbConstant.oracleDriver);
            }else {
                System.out.println("Error db type");
            }
            connection = (Connection) DriverManager.getConnection(url,username,password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private static void getTableAllData(String dbType, String url, String username, String password, String tableName){
        Connection connection = getDbConnection(dbType,url,username,password);
        String sql = "Select * from " + tableName;
        PreparedStatement preparedStatement;
        try {
            preparedStatement = (PreparedStatement)connection.prepareStatement(sql);
            long before = System.currentTimeMillis();
            ResultSet resultSet = preparedStatement.executeQuery();
            int col = resultSet.getMetaData().getColumnCount();
            resultSet.last();
            System.out.println("Count cost time: " + (System.currentTimeMillis() - before) + "total: " + resultSet.getRow());
            resultSet.first();
            System.out.println("============================");
            while (resultSet.next()){
                for (int i = 1; i <= col; i++){
                    System.out.print(resultSet.getString(i) + "\t");
                    if ((i==2)&&(resultSet.getString(i).length() < 8)){
                        System.out.print("\t");
                    }
                }
                System.out.println("");
            }
            System.out.println("============================");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void getTablePartialData(String dbType, String url, String username, String password, String tableName, List<String> fields){
        Connection connection = getDbConnection(dbType,url,username,password);
        String sql = "Select ";
        for (String field:fields){
            sql += field + " , ";
        }
        sql = sql.substring(0,sql.length()-2);
        sql += "from " + tableName + ";";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            int col = resultSet.getMetaData().getColumnCount();
            for (int i=1;i<=col;i++){
                System.out.print(resultSet.getMetaData().getColumnName(i)+"\t");
                if ((i==2)&&(resultSet.getMetaData().getColumnName(i).length()<8)){
                    System.out.print("\t");
                }
            }
            System.out.println("");
            while (resultSet.next()){
                for (int i = 1; i <= col; i++){
                    System.out.print(resultSet.getString(i)+"\t");
                    if ((i==2)&&(resultSet.getString(i).length()<8)){
                        System.out.print("\t");
                    }
                }
                System.out.println("");
            }
            System.out.println("============================");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static List<String> getDbTables(String dbType, String url, String username, String password) throws SQLException {
        List tables = new ArrayList();
        Connection connection = getDbConnection(dbType,url,username,password);
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getTables(connection.getCatalog(),username,null,new String[]{"TABLE"});
        while (resultSet.next()){
            tables.add(resultSet.getString("TABLE_NAME"));
            System.out.println(resultSet.getString("TABLE_NAME"));
        }
        return tables;
    }

}
