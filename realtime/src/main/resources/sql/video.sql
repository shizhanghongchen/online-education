
CREATE TABLE `video_learn_detail` (
  `userid` INT(11) NOT NULL DEFAULT '0',
  `cwareid` INT(11) NOT NULL DEFAULT '0',
  `videoid` INT(11) NOT NULL DEFAULT '0',
  `totaltime` BIGINT(20) DEFAULT NULL,
  `effecttime` BIGINT(20) DEFAULT NULL,
  `completetime` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`userid`,`cwareid`,`videoid`)
) ENGINE=INNODB DEFAULT CHARSET=utf8

CREATE TABLE `chapter_learn_detail` (
  `chapterid` INT(11) NOT NULL DEFAULT '0',
  `totaltime` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`chapterid`)
) ENGINE=INNODB DEFAULT CHARSET=utf8

CREATE TABLE `cwareid_learn_detail` (
  `cwareid` INT(11) NOT NULL DEFAULT '0',
  `totaltime` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`cwareid`)
) ENGINE=INNODB DEFAULT CHARSET=utf8


CREATE TABLE `edutype_learn_detail` (
  `edutypeid` INT(11) NOT NULL DEFAULT '0',
  `totaltime` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`edutypeid`)
) ENGINE=INNODB DEFAULT CHARSET=utf8


CREATE TABLE `sourcetype_learn_detail` (
  `sourcetype` VARCHAR(10) NOT NULL DEFAULT '',
  `totaltime` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`sourcetype_learn`)
) ENGINE=INNODB DEFAULT CHARSET=utf8

CREATE TABLE `subject_learn_detail` (
  `sourcetype_learn` INT(11) NOT NULL DEFAULT '0',
  `totaltime` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`subjectid`)
) ENGINE=INNODB DEFAULT CHARSET=utf8

CREATE TABLE `video_interval` (
  `userid` INT(11) NOT NULL DEFAULT '0',
  `cwareid` INT(11) NOT NULL DEFAULT '0',
  `videoid` INT(11) NOT NULL DEFAULT '0',
  `play_interval` TEXT,
  PRIMARY KEY (`userid`,`cwareid`,`videoid`)
) ENGINE=INNODB DEFAULT CHARSET=utf8