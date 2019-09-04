-- ----------------------------
-- Table structure for offset_manager
-- ----------------------------
DROP TABLE IF EXISTS `offset_manager`;
CREATE TABLE `offset_manager` (
  `groupid` varchar(50) DEFAULT NULL,
  `topic` varchar(50) DEFAULT NULL,
  `partition` int(11) DEFAULT NULL,
  `untiloffset` mediumtext,
  UNIQUE KEY `offset_unique` (`groupid`,`topic`,`partition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for qz_point_detail
-- ----------------------------
DROP TABLE IF EXISTS `qz_point_detail`;
CREATE TABLE `qz_point_detail` (
  `userid` int(11) DEFAULT NULL,
  `courseid` int(11) DEFAULT NULL,
  `pointid` int(11) DEFAULT NULL,
  `qz_sum` int(11) DEFAULT NULL,
  `qz_count` int(11) DEFAULT NULL,
  `qz_istrue` int(11) DEFAULT NULL,
  `correct_rate` double(4,2) DEFAULT NULL,
  `mastery_rate` double(4,2) DEFAULT NULL,
  `createtime` datetime DEFAULT NULL,
  `updatetime` datetime DEFAULT NULL,
  UNIQUE KEY `qz_point_detail_unique` (`userid`,`courseid`,`pointid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for qz_point_history
-- ----------------------------
DROP TABLE IF EXISTS `qz_point_history`;
CREATE TABLE `qz_point_history` (
  `userid` int(11) DEFAULT NULL,
  `courseid` int(11) DEFAULT NULL,
  `pointid` int(11) DEFAULT NULL,
  `questionids` text,
  `createtime` datetime DEFAULT NULL,
  `updatetime` datetime DEFAULT NULL,
  UNIQUE KEY `qz_point_set_unique` (`userid`,`courseid`,`pointid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;