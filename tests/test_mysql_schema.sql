# Create database and tables for mysqlhandler tests
# Julian Briggs
# 22-jan-2022

Drop table if exists testtable;
CREATE TABLE if not exists testtable (
  id int NOT NULL AUTO_INCREMENT,
  first_name varchar(45) default "A" NOT NULL,
  last_name varchar(45) default "B"  NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE if not exists `reading30compact` (
  `date` timestamp NOT NULL DEFAULT '1970-01-02 00:00:00',
  `ss_id` int unsigned NOT NULL,
  `t1` float DEFAULT NULL,
  `t2` float DEFAULT NULL,
  `t3` float DEFAULT NULL,
  `t4` float DEFAULT NULL,
  `t5` float DEFAULT NULL,
  `t6` float DEFAULT NULL,
  `t7` float DEFAULT NULL,
  `t8` float DEFAULT NULL,
  `t9` float DEFAULT NULL,
  `t10` float DEFAULT NULL,
  `t11` float DEFAULT NULL,
  `t12` float DEFAULT NULL,
  `t13` float DEFAULT NULL,
  `t14` float DEFAULT NULL,
  `t15` float DEFAULT NULL,
  `t16` float DEFAULT NULL,
  `t17` float DEFAULT NULL,
  `t18` float DEFAULT NULL,
  `t19` float DEFAULT NULL,
  `t20` float DEFAULT NULL,
  `t21` float DEFAULT NULL,
  `t22` float DEFAULT NULL,
  `t23` float DEFAULT NULL,
  `t24` float DEFAULT NULL,
  `t25` float DEFAULT NULL,
  `t26` float DEFAULT NULL,
  `t27` float DEFAULT NULL,
  `t28` float DEFAULT NULL,
  `t29` float DEFAULT NULL,
  `t30` float DEFAULT NULL,
  `t31` float DEFAULT NULL,
  `t32` float DEFAULT NULL,
  `t33` float DEFAULT NULL,
  `t34` float DEFAULT NULL,
  `t35` float DEFAULT NULL,
  `t36` float DEFAULT NULL,
  `t37` float DEFAULT NULL,
  `t38` float DEFAULT NULL,
  `t39` float DEFAULT NULL,
  `t40` float DEFAULT NULL,
  `t41` float DEFAULT NULL,
  `t42` float DEFAULT NULL,
  `t43` float DEFAULT NULL,
  `t44` float DEFAULT NULL,
  `t45` float DEFAULT NULL,
  `t46` float DEFAULT NULL,
  `t47` float DEFAULT NULL,
  `t48` float DEFAULT NULL,
  PRIMARY KEY (`date`,`ss_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 ROW_FORMAT=DYNAMIC COMMENT='Prepared Passiv Historic data: SSF ss_id (not Passiv install_id). Omits cols: meter_id,missing_periods, daily_total';

