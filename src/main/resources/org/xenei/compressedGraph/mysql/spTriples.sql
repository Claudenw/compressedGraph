CREATE TABLE `spTriples` (
  `subject` int(11) NOT NULL,
  `predicate` int(11) NOT NULL,
  `object` int(11) NOT NULL,
  PRIMARY KEY (`subject`,`predicate`,`object`),
  KEY `PO` (`predicate`,`object`) USING BTREE,
  KEY `O` (`object`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (subject)
PARTITIONS 16 */