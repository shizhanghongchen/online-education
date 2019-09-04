-- ----------------------------
-- Table structure for ads_user_paper_detail
-- ----------------------------
DROP TABLE IF EXISTS `ads_user_paper_detail`;
CREATE TABLE `ads_user_paper_detail` (
  `paperviewid` varchar(20) DEFAULT NULL,
  `paperviewname` varchar(20) DEFAULT NULL,
  `unpasscount` varchar(20) DEFAULT NULL,
  `passcount` varchar(20) DEFAULT NULL,
  `rate` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_user_question_detail
-- ----------------------------
DROP TABLE IF EXISTS `ads_user_question_detail`;
CREATE TABLE `ads_user_question_detail` (
  `questionid` varchar(20) DEFAULT NULL,
  `errcount` varchar(20) DEFAULT NULL,
  `rightcount` varchar(20) DEFAULT NULL,
  `rate` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_low3_userdetail
-- ----------------------------
DROP TABLE IF EXISTS `ads_low3_userdetail`;
CREATE TABLE `ads_low3_userdetail` (
  `userid` varchar(20) DEFAULT NULL,
  `paperviewid` varchar(20) DEFAULT NULL,
  `paperviewname` varchar(20) DEFAULT NULL,
  `chaptername` varchar(20) DEFAULT NULL,
  `pointname` varchar(20) DEFAULT NULL,
  `sitecoursename` varchar(20) DEFAULT NULL,
  `coursename` varchar(20) DEFAULT NULL,
  `majorname` varchar(20) DEFAULT NULL,
  `shortname` varchar(20) DEFAULT NULL,
  `papername` varchar(20) DEFAULT NULL,
  `score` varchar(20) DEFAULT NULL,
  `rk` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_paper_avgtimeandscore
-- ----------------------------
DROP TABLE IF EXISTS `ads_paper_avgtimeandscore`;
CREATE TABLE `ads_paper_avgtimeandscore` (
  `paperviewid` varchar(20) DEFAULT NULL,
  `paperviewname` varchar(20) DEFAULT NULL,
  `avgscore` varchar(20) DEFAULT NULL,
  `avgspendtime` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_paper_maxdetail
-- ----------------------------
DROP TABLE IF EXISTS `ads_paper_maxdetail`;
CREATE TABLE `ads_paper_maxdetail` (
  `paperviewid` varchar(20) DEFAULT NULL,
  `paperviewname` varchar(20) DEFAULT NULL,
  `maxscore` varchar(20) DEFAULT NULL,
  `minscore` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_paper_scoresegment_user
-- ----------------------------
DROP TABLE IF EXISTS `ads_paper_scoresegment_user`;
CREATE TABLE `ads_paper_scoresegment_user` (
  `paperviewid` varchar(20) DEFAULT NULL,
  `paperviewname` varchar(20) DEFAULT NULL,
  `score_segment` varchar(20) DEFAULT NULL,
  `userids` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_top3_userdetail
-- ----------------------------
DROP TABLE IF EXISTS `ads_top3_userdetail`;
CREATE TABLE `ads_top3_userdetail` (
  `userid` varchar(20) DEFAULT NULL,
  `paperviewid` varchar(20) DEFAULT NULL,
  `paperviewname` varchar(20) DEFAULT NULL,
  `chaptername` varchar(20) DEFAULT NULL,
  `pointname` varchar(20) DEFAULT NULL,
  `sitecoursename` varchar(20) DEFAULT NULL,
  `coursename` varchar(20) DEFAULT NULL,
  `majorname` varchar(20) DEFAULT NULL,
  `shortname` varchar(20) DEFAULT NULL,
  `papername` varchar(20) DEFAULT NULL,
  `score` varchar(20) DEFAULT NULL,
  `rk` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_register_adnamenum
-- ----------------------------
DROP TABLE IF EXISTS `ads_register_adnamenum`;
CREATE TABLE `ads_register_adnamenum` (
  `adname` varchar(20) DEFAULT NULL,
  `num` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_register_appregurlnum
-- ----------------------------
DROP TABLE IF EXISTS `ads_register_appregurlnum`;
CREATE TABLE `ads_register_appregurlnum` (
  `appregurl` varchar(20) DEFAULT NULL,
  `num` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_register_memberlevelnum
-- ----------------------------
DROP TABLE IF EXISTS `ads_register_memberlevelnum`;
CREATE TABLE `ads_register_memberlevelnum` (
  `memberlevel` varchar(20) DEFAULT NULL,
  `num` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_register_regsourcenamenum
-- ----------------------------
DROP TABLE IF EXISTS `ads_register_regsourcenamenum`;
CREATE TABLE `ads_register_regsourcenamenum` (
  `regsourcename` varchar(20) DEFAULT NULL,
  `num` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_register_sitenamenum
-- ----------------------------
DROP TABLE IF EXISTS `ads_register_sitenamenum`;
CREATE TABLE `ads_register_sitenamenum` (
  `sitename` varchar(20) DEFAULT NULL,
  `num` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_register_top3memberpay
-- ----------------------------
DROP TABLE IF EXISTS `ads_register_top3memberpay`;
CREATE TABLE `ads_register_top3memberpay` (
  `uid` varchar(20) DEFAULT NULL,
  `memberlevel` varchar(20) DEFAULT NULL,
  `register` varchar(20) DEFAULT NULL,
  `appregurl` varchar(20) DEFAULT NULL,
  `regsourcename` varchar(20) DEFAULT NULL,
  `adname` varchar(20) DEFAULT NULL,
  `sitename` varchar(20) DEFAULT NULL,
  `vip_level` varchar(20) DEFAULT NULL,
  `paymoney` varchar(20) DEFAULT NULL,
  `rownum` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ads_register_viplevelnum
-- ----------------------------
DROP TABLE IF EXISTS `ads_register_viplevelnum`;
CREATE TABLE `ads_register_viplevelnum` (
  `vip_level` varchar(20) DEFAULT NULL,
  `num` varchar(20) DEFAULT NULL,
  `dt` varchar(20) DEFAULT NULL,
  `dn` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




