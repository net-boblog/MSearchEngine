package com.midea.cloudSearch.druid.prase;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.midea.cloudSearch.druid.segment.Condition;
import com.midea.cloudSearch.druid.segment.Field;
import com.midea.cloudSearch.druid.segment.From;
import com.midea.cloudSearch.druid.segment.Hint;
import com.midea.cloudSearch.druid.segment.HintType;
import com.midea.cloudSearch.druid.segment.JoinSelect;
import com.midea.cloudSearch.druid.segment.MethodField;
import com.midea.cloudSearch.druid.segment.Order;
import com.midea.cloudSearch.druid.segment.Select;
import com.midea.cloudSearch.druid.segment.Where;
import com.midea.cloudSearch.exception.SqlParseException;


public class SqlParserTests {
    private static SqlParser parser;

    @BeforeClass
    public static void init(){
        parser = new SqlParser();
    }

    @Test
    public void joinParseCheckSelectedFieldsSplit() throws SqlParseException {
    	
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";
        
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        
        List<Field> t1Fields = joinSelect.getFirstTable().getSelectedFields();
        Assert.assertEquals(t1Fields.size(),3);
        Assert.assertTrue(fieldExist(t1Fields, "firstname"));
        Assert.assertTrue(fieldExist(t1Fields, "lastname"));
        Assert.assertTrue(fieldExist(t1Fields, "gender"));
        List<Field> t2Fields = joinSelect.getSecondTable().getSelectedFields();
        Assert.assertEquals(t2Fields.size(),2);
        Assert.assertTrue(fieldExist(t2Fields,"holdersName"));
        Assert.assertTrue(fieldExist(t2Fields,"name"));
    }
    
    
    
    
    

    @Test
    public void joinParseCheckConnectedFields() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));

        List<Field> t1Fields = joinSelect.getFirstTable().getConnectedFields();
        Assert.assertEquals(t1Fields.size(),2);
        Assert.assertTrue(fieldExist(t1Fields, "firstname"));
        Assert.assertTrue(fieldExist(t1Fields, "age"));

        List<Field> t2Fields = joinSelect.getSecondTable().getConnectedFields();
        Assert.assertEquals(t2Fields.size(),2);
        Assert.assertTrue(fieldExist(t2Fields,"holdersName"));
        Assert.assertTrue(fieldExist(t2Fields,"age"));
    }

    private boolean fieldExist(List<Field> fields, String fieldName) {
        for(Field field : fields)
            if(field.getName().equals(fieldName)) return true;

        return false;
    }


    @Test
    public void joinParseFromsAreSplitedCorrectly() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<From> t1From = joinSelect.getFirstTable().getFrom();

        Assert.assertNotNull(t1From);
        Assert.assertEquals(1,t1From.size());
        Assert.assertTrue(checkFrom(t1From.get(0),"elasticsearch-sql_test_index","account","a"));

        List<From> t2From = joinSelect.getSecondTable().getFrom();
        Assert.assertNotNull(t2From);
        Assert.assertEquals(1,t2From.size());
        Assert.assertTrue(checkFrom(t2From.get(0),"elasticsearch-sql_test_index","dog","d"));
    }

    private boolean checkFrom(From from, String index, String type, String alias) {
        return from.getAlias().equals(alias) && from.getIndex().equals(index)
                && from.getType().equals(type);
    }


  


    @Test
    public void joinSplitWhereCorrectly() throws SqlParseException {
        String query = "SELECT a.*, a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        String s1Where = joinSelect.getFirstTable().getWhere().toString();
        Assert.assertEquals("AND ( AND firstname EQ eliran, AND ( OR age GT 10, OR balance GT 2000 )  ) " , s1Where);
        String s2Where = joinSelect.getSecondTable().getWhere().toString();
        Assert.assertEquals("AND age GT 1",s2Where);
    }

    
    private SQLExpr queryToExpr(String query) {
        return new ElasticSqlExprParser(query).expr();
    }

}
