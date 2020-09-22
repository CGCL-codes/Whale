/*
Navicat MySQL Data Transfer

Source Server         : 华科ubuntu2
Source Server Version : 50721
Source Host           : ubuntu2:3306
Source Database       : whale

Target Server Type    : MYSQL
Target Server Version : 50721
File Encoding         : 65001

Date: 2018-03-13 10:58:03
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for t_latency
-- ----------------------------
DROP TABLE IF EXISTS `t_throughput`;
CREATE TABLE `t_throughput` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `taskid` int(11) NOT NULL,
  `throughput` bigint(20) NOT NULL,
  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=11938 DEFAULT CHARSET=utf8;
