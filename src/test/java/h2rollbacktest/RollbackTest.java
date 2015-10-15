package h2rollbacktest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.jooq.SQLDialect;
import org.jooq.TransactionalRunnable;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RollbackTest {

  @Before
  public void setup() {
    withTransaction(configuration -> {
      DSL.using(configuration)
         .createTable("TEST")
         .column("ID", SQLDataType.BIGINT.nullable(false))
         .execute();
      DSL.using(configuration)
         .alterTable("TEST")
         .add(DSL.constraint("PK").primaryKey("ID"))
         .execute();
    });
  }

  @After
  public void cleanup() {
    withTransaction(configuration -> {
      DSL.using(configuration)
         .dropTable("TEST")
         .execute();
    });
  }

  @Test
  public void testDelete() {
    withTransaction(configuration -> {
      DSL.using(configuration)
         .query("insert into TEST (ID) values (1)")
         .execute();
    });
    withTransaction(configuration -> {
      DSL.using(configuration)
         .query("delete from TEST where ID=1")
         .execute();
    });
  }

  @Test
  public void testDeleteAfterRollback() {
    withTransaction(configuration -> {
      DSL.using(configuration)
         .query("insert into TEST (ID) values (1)")
         .execute();
    });
    withTransaction(configuration -> {
      DSL.using(configuration)
         .query("insert into TEST (ID) values (1)")
         .execute();
    });
  }

  private Connection get() {
    String url = "jdbc:h2:mem:runtime;DB_CLOSE_DELAY=-1";
    Properties props = new Properties();

    try {
      Connection connection = DriverManager.getConnection(url, props);
      connection.setAutoCommit(false);
      return connection;
    }
    catch (SQLException e) {
      throw new DataAccessException("no connection possible", e);
    }
  }

  private void withTransaction(TransactionalRunnable transactionalCode) {
    try (Connection connection = get()) {
      DSL.using(connection, SQLDialect.H2).transaction(transactionalCode);
    }
    catch (SQLException e) {
      throw new DataAccessException("no transaction possible", e);
    }
  }
}
