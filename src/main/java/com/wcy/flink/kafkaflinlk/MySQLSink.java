package com.wcy.flink.kafkaflinlk;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author wcy
 * @date 2019/9/26 16:39
 * @Description:
 */
public class MySQLSink extends RichSinkFunction<Message> {
    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username="root";
    String password="123456";
    String drivername="com.mysql.jdbc.Driver";
    String dburl="jdbc:mysql://127.0.0.1:3306/world?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=UTC";

    @Override
    public void invoke(Message value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into user_k(id,msg,sendTime) values(?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, value.getId());
        preparedStatement.setString(2, value.getMsg());
        preparedStatement.setString(3, value.getSendTime());
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
}
