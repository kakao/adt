CREATE TABLE `%s`.`adt_test_2` (
  `no` int(11) NOT NULL,
  `seq` int(11) NOT NULL,
  `uk` int(11) NOT NULL,
  `v` text NOT NULL,
  `c` int(11) NOT NULL,
  `modtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `regtime` datetime NOT NULL,
  PRIMARY KEY (`no`),
  UNIQUE KEY `ux_uk_no` (`uk`),
  KEY `ix_modtime` (`modtime`),
  KEY `ix_regtime` (`regtime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;