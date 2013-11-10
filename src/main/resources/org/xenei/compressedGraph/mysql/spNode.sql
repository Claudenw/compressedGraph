CREATE TABLE `spNode` (
  `idx` int(11) NOT NULL AUTO_INCREMENT,
  `data` blob NOT NULL,
  PRIMARY KEY (`idx`),
  KEY `node_data` (`data`(80))
) ENGINE=InnoDB;