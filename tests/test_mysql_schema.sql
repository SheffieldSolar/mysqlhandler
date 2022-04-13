# Create database and tables for mysqlhandler tests
# Julian Briggs
# 22-jan-2022

Drop table if exists testtable;
CREATE TABLE if not exists testtable (
  id int NOT NULL AUTO_INCREMENT,
  first_name varchar(45) NOT NULL,
  last_name varchar(45) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
