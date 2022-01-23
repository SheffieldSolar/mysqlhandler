# Create database and tables for mysql_handler tests
# Julian Briggs
# 22-jan-2022

Drop table if exists test.testtable;
CREATE TABLE if not exists test.testtable (
  id int NOT NULL AUTO_INCREMENT,
  first_name varchar(45) NOT NULL,
  last_name varchar(45) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
