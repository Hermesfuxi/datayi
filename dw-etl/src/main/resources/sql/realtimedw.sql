/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : realtimedw

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2020-07-05 19:56:31
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for couponfetchinfo
-- ----------------------------
DROP TABLE IF EXISTS `couponfetchinfo`;
CREATE TABLE `couponfetchinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `userid` int(11) DEFAULT NULL,
  `couponid` int(11) DEFAULT NULL,
  `couponcreatetime` varchar(255) DEFAULT NULL,
  `fetchtime` varchar(255) DEFAULT NULL,
  `couponstatus` varchar(255) DEFAULT NULL COMMENT '1已领取未使用 2已使用  3已过期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of couponfetchinfo
-- ----------------------------

-- ----------------------------
-- Table structure for couponinfo
-- ----------------------------
DROP TABLE IF EXISTS `couponinfo`;
CREATE TABLE `couponinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `couponName` varchar(255) DEFAULT NULL COMMENT '1满减 2折扣券 3新人券 4现金券 5免单券 6包邮券 7指定活动券',
  `couponType` varchar(255) DEFAULT NULL,
  `couponTime` varchar(255) DEFAULT NULL,
  `couponStatus` int(255) DEFAULT NULL,
  `amount` double(255,0) DEFAULT NULL,
  `approveTime` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of couponinfo
-- ----------------------------

-- ----------------------------
-- Table structure for dim_adinfo
-- ----------------------------
DROP TABLE IF EXISTS `dim_adinfo`;
CREATE TABLE `dim_adinfo` (
  `id` int(11) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `campain` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dim_adinfo
-- ----------------------------

-- ----------------------------
-- Table structure for dim_campain_info
-- ----------------------------
DROP TABLE IF EXISTS `dim_campain_info`;
CREATE TABLE `dim_campain_info` (
  `id` int(11) NOT NULL,
  `campain_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dim_campain_info
-- ----------------------------

-- ----------------------------
-- Table structure for dim_lanmu
-- ----------------------------
DROP TABLE IF EXISTS `dim_lanmu`;
CREATE TABLE `dim_lanmu` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dim_lanmu
-- ----------------------------
INSERT INTO `dim_lanmu` VALUES ('1', 'lm_a');
INSERT INTO `dim_lanmu` VALUES ('2', 'lm_b');
INSERT INTO `dim_lanmu` VALUES ('3', 'lm_c');
INSERT INTO `dim_lanmu` VALUES ('4', 'lm_d');
INSERT INTO `dim_lanmu` VALUES ('5', 'lm_e');
INSERT INTO `dim_lanmu` VALUES ('6', 'lm_f');
INSERT INTO `dim_lanmu` VALUES ('7', 'lm_g');
INSERT INTO `dim_lanmu` VALUES ('8', 'lm_h');
INSERT INTO `dim_lanmu` VALUES ('9', 'lm_k');
INSERT INTO `dim_lanmu` VALUES ('10', 'lm_m');
INSERT INTO `dim_lanmu` VALUES ('11', 'lm_n');
INSERT INTO `dim_lanmu` VALUES ('12', 'lm_o');
INSERT INTO `dim_lanmu` VALUES ('13', 'lm_p');
INSERT INTO `dim_lanmu` VALUES ('14', 'lm_q');
INSERT INTO `dim_lanmu` VALUES ('15', 'lm_r');
INSERT INTO `dim_lanmu` VALUES ('16', 'lm_s');
INSERT INTO `dim_lanmu` VALUES ('17', 'lm_t');
INSERT INTO `dim_lanmu` VALUES ('18', 'lm_u');
INSERT INTO `dim_lanmu` VALUES ('19', 'lm_v');
INSERT INTO `dim_lanmu` VALUES ('20', 'lm_w');
INSERT INTO `dim_lanmu` VALUES ('21', 'lm_x');

-- ----------------------------
-- Table structure for dim_pginfo
-- ----------------------------
DROP TABLE IF EXISTS `dim_pginfo`;
CREATE TABLE `dim_pginfo` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `pindao` int(10) DEFAULT NULL,
  `lanmu` int(10) DEFAULT NULL,
  `pgtype` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2001 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dim_pginfo
-- ----------------------------
INSERT INTO `dim_pginfo` VALUES ('1', '5', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('2', '13', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('3', '1', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('4', '13', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('5', '11', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('6', '3', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('7', '5', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('8', '4', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('9', '7', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('10', '3', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('11', '9', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('12', '11', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('13', '3', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('14', '5', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('15', '1', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('16', '2', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('17', '7', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('18', '8', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('19', '9', '5', '2');
INSERT INTO `dim_pginfo` VALUES ('20', '4', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('21', '3', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('22', '13', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('23', '5', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('24', '10', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('25', '10', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('26', '1', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('27', '1', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('28', '12', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('29', '6', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('30', '10', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('31', '9', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('32', '7', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('33', '4', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('34', '9', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('35', '6', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('36', '12', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('37', '6', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('38', '11', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('39', '8', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('40', '12', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('41', '10', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('42', '9', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('43', '6', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('44', '4', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('45', '7', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('46', '8', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('47', '7', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('48', '3', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('49', '3', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('50', '3', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('51', '4', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('52', '3', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('53', '5', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('54', '11', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('55', '12', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('56', '1', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('57', '9', '5', '2');
INSERT INTO `dim_pginfo` VALUES ('58', '7', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('59', '8', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('60', '5', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('61', '6', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('62', '4', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('63', '4', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('64', '6', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('65', '1', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('66', '4', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('67', '7', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('68', '6', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('69', '7', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('70', '8', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('71', '7', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('72', '3', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('73', '1', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('74', '5', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('75', '2', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('76', '11', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('77', '13', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('78', '3', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('79', '7', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('80', '2', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('81', '2', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('82', '13', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('83', '7', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('84', '4', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('85', '7', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('86', '10', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('87', '8', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('88', '7', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('89', '9', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('90', '7', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('91', '3', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('92', '3', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('93', '5', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('94', '6', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('95', '13', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('96', '5', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('97', '4', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('98', '4', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('99', '3', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('100', '7', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('101', '9', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('102', '8', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('103', '1', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('104', '5', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('105', '3', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('106', '12', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('107', '3', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('108', '11', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('109', '8', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('110', '7', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('111', '1', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('112', '4', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('113', '9', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('114', '2', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('115', '2', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('116', '3', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('117', '8', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('118', '4', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('119', '9', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('120', '7', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('121', '11', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('122', '12', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('123', '11', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('124', '2', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('125', '11', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('126', '8', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('127', '7', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('128', '5', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('129', '2', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('130', '6', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('131', '4', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('132', '12', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('133', '2', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('134', '4', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('135', '7', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('136', '9', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('137', '11', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('138', '5', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('139', '2', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('140', '9', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('141', '10', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('142', '1', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('143', '6', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('144', '1', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('145', '8', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('146', '13', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('147', '11', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('148', '10', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('149', '3', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('150', '12', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('151', '2', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('152', '10', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('153', '13', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('154', '11', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('155', '5', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('156', '8', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('157', '8', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('158', '10', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('159', '12', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('160', '3', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('161', '12', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('162', '2', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('163', '11', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('164', '12', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('165', '8', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('166', '1', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('167', '4', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('168', '9', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('169', '13', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('170', '9', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('171', '5', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('172', '10', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('173', '5', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('174', '8', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('175', '7', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('176', '4', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('177', '2', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('178', '4', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('179', '2', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('180', '13', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('181', '7', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('182', '10', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('183', '13', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('184', '13', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('185', '9', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('186', '5', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('187', '13', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('188', '5', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('189', '12', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('190', '9', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('191', '12', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('192', '2', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('193', '4', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('194', '5', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('195', '3', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('196', '6', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('197', '8', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('198', '11', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('199', '1', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('200', '13', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('201', '11', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('202', '1', '4', '6');
INSERT INTO `dim_pginfo` VALUES ('203', '7', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('204', '10', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('205', '6', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('206', '1', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('207', '8', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('208', '7', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('209', '2', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('210', '9', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('211', '9', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('212', '5', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('213', '8', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('214', '9', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('215', '4', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('216', '8', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('217', '10', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('218', '5', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('219', '13', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('220', '3', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('221', '10', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('222', '11', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('223', '5', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('224', '4', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('225', '7', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('226', '7', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('227', '6', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('228', '12', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('229', '10', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('230', '13', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('231', '7', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('232', '11', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('233', '1', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('234', '10', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('235', '8', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('236', '8', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('237', '2', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('238', '7', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('239', '3', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('240', '13', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('241', '6', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('242', '2', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('243', '8', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('244', '11', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('245', '4', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('246', '6', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('247', '10', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('248', '1', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('249', '13', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('250', '3', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('251', '3', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('252', '13', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('253', '5', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('254', '13', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('255', '8', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('256', '8', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('257', '4', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('258', '2', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('259', '5', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('260', '6', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('261', '5', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('262', '10', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('263', '2', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('264', '12', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('265', '4', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('266', '12', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('267', '10', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('268', '2', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('269', '2', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('270', '4', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('271', '11', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('272', '4', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('273', '2', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('274', '9', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('275', '4', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('276', '11', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('277', '5', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('278', '1', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('279', '7', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('280', '8', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('281', '7', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('282', '3', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('283', '10', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('284', '3', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('285', '1', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('286', '9', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('287', '7', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('288', '8', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('289', '12', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('290', '11', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('291', '5', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('292', '11', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('293', '13', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('294', '1', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('295', '1', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('296', '6', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('297', '4', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('298', '2', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('299', '8', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('300', '4', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('301', '10', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('302', '1', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('303', '1', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('304', '3', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('305', '5', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('306', '12', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('307', '4', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('308', '3', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('309', '2', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('310', '6', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('311', '2', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('312', '12', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('313', '10', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('314', '13', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('315', '7', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('316', '3', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('317', '8', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('318', '12', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('319', '8', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('320', '13', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('321', '9', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('322', '10', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('323', '9', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('324', '9', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('325', '12', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('326', '7', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('327', '6', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('328', '2', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('329', '9', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('330', '11', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('331', '1', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('332', '9', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('333', '10', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('334', '4', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('335', '12', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('336', '5', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('337', '7', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('338', '4', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('339', '7', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('340', '13', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('341', '13', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('342', '10', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('343', '6', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('344', '3', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('345', '8', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('346', '4', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('347', '10', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('348', '12', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('349', '3', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('350', '10', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('351', '6', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('352', '5', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('353', '13', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('354', '10', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('355', '8', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('356', '2', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('357', '5', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('358', '12', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('359', '13', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('360', '11', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('361', '8', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('362', '1', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('363', '4', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('364', '13', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('365', '9', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('366', '7', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('367', '1', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('368', '1', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('369', '8', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('370', '1', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('371', '1', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('372', '11', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('373', '2', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('374', '13', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('375', '7', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('376', '13', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('377', '13', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('378', '11', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('379', '8', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('380', '12', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('381', '10', '4', '6');
INSERT INTO `dim_pginfo` VALUES ('382', '10', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('383', '13', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('384', '13', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('385', '1', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('386', '13', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('387', '3', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('388', '10', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('389', '5', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('390', '3', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('391', '1', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('392', '8', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('393', '3', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('394', '12', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('395', '10', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('396', '13', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('397', '7', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('398', '7', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('399', '6', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('400', '12', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('401', '2', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('402', '3', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('403', '7', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('404', '1', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('405', '1', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('406', '8', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('407', '4', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('408', '7', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('409', '4', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('410', '1', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('411', '12', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('412', '3', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('413', '1', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('414', '2', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('415', '2', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('416', '3', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('417', '5', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('418', '13', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('419', '11', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('420', '9', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('421', '13', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('422', '9', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('423', '3', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('424', '1', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('425', '9', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('426', '12', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('427', '13', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('428', '2', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('429', '2', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('430', '8', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('431', '2', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('432', '10', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('433', '11', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('434', '9', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('435', '5', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('436', '9', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('437', '3', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('438', '8', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('439', '2', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('440', '11', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('441', '13', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('442', '10', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('443', '12', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('444', '3', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('445', '8', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('446', '2', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('447', '2', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('448', '1', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('449', '7', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('450', '10', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('451', '13', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('452', '2', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('453', '2', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('454', '4', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('455', '5', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('456', '6', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('457', '8', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('458', '12', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('459', '13', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('460', '11', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('461', '12', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('462', '3', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('463', '9', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('464', '5', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('465', '11', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('466', '7', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('467', '9', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('468', '9', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('469', '1', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('470', '1', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('471', '2', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('472', '10', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('473', '13', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('474', '13', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('475', '13', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('476', '13', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('477', '7', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('478', '12', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('479', '13', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('480', '12', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('481', '2', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('482', '2', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('483', '10', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('484', '8', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('485', '1', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('486', '1', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('487', '12', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('488', '4', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('489', '4', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('490', '11', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('491', '4', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('492', '13', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('493', '8', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('494', '3', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('495', '7', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('496', '2', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('497', '5', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('498', '13', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('499', '9', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('500', '9', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('501', '1', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('502', '3', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('503', '7', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('504', '2', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('505', '4', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('506', '10', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('507', '7', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('508', '7', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('509', '2', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('510', '12', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('511', '11', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('512', '4', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('513', '2', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('514', '4', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('515', '13', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('516', '6', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('517', '3', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('518', '1', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('519', '3', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('520', '11', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('521', '3', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('522', '8', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('523', '3', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('524', '13', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('525', '11', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('526', '2', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('527', '1', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('528', '10', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('529', '4', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('530', '7', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('531', '5', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('532', '5', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('533', '9', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('534', '3', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('535', '5', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('536', '13', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('537', '3', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('538', '6', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('539', '7', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('540', '9', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('541', '3', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('542', '12', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('543', '2', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('544', '3', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('545', '11', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('546', '2', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('547', '9', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('548', '3', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('549', '7', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('550', '9', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('551', '2', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('552', '7', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('553', '12', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('554', '1', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('555', '3', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('556', '8', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('557', '9', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('558', '9', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('559', '10', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('560', '2', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('561', '8', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('562', '9', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('563', '7', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('564', '11', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('565', '7', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('566', '3', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('567', '8', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('568', '1', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('569', '4', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('570', '1', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('571', '1', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('572', '8', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('573', '13', '5', '2');
INSERT INTO `dim_pginfo` VALUES ('574', '1', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('575', '1', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('576', '5', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('577', '5', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('578', '8', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('579', '10', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('580', '4', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('581', '1', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('582', '1', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('583', '6', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('584', '2', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('585', '3', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('586', '7', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('587', '11', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('588', '3', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('589', '5', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('590', '10', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('591', '6', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('592', '6', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('593', '1', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('594', '8', '19', '3');
INSERT INTO `dim_pginfo` VALUES ('595', '2', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('596', '4', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('597', '2', '4', '6');
INSERT INTO `dim_pginfo` VALUES ('598', '3', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('599', '3', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('600', '2', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('601', '5', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('602', '1', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('603', '11', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('604', '3', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('605', '2', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('606', '4', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('607', '10', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('608', '10', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('609', '1', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('610', '11', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('611', '8', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('612', '13', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('613', '8', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('614', '4', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('615', '2', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('616', '11', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('617', '12', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('618', '9', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('619', '3', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('620', '3', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('621', '9', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('622', '13', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('623', '11', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('624', '11', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('625', '3', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('626', '1', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('627', '12', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('628', '6', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('629', '9', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('630', '13', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('631', '1', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('632', '10', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('633', '4', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('634', '12', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('635', '8', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('636', '3', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('637', '4', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('638', '12', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('639', '3', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('640', '8', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('641', '2', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('642', '4', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('643', '13', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('644', '10', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('645', '6', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('646', '6', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('647', '13', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('648', '2', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('649', '10', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('650', '10', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('651', '11', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('652', '12', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('653', '10', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('654', '10', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('655', '1', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('656', '7', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('657', '2', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('658', '12', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('659', '5', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('660', '3', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('661', '6', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('662', '13', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('663', '12', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('664', '9', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('665', '1', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('666', '1', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('667', '7', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('668', '5', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('669', '10', '19', '3');
INSERT INTO `dim_pginfo` VALUES ('670', '4', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('671', '10', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('672', '2', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('673', '6', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('674', '9', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('675', '11', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('676', '11', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('677', '8', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('678', '7', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('679', '5', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('680', '9', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('681', '9', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('682', '3', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('683', '8', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('684', '7', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('685', '12', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('686', '1', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('687', '4', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('688', '8', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('689', '11', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('690', '8', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('691', '7', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('692', '1', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('693', '7', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('694', '3', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('695', '4', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('696', '1', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('697', '9', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('698', '5', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('699', '13', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('700', '1', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('701', '12', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('702', '5', '19', '3');
INSERT INTO `dim_pginfo` VALUES ('703', '12', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('704', '9', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('705', '11', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('706', '12', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('707', '5', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('708', '11', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('709', '6', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('710', '4', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('711', '2', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('712', '12', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('713', '8', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('714', '1', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('715', '11', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('716', '11', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('717', '10', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('718', '11', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('719', '2', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('720', '7', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('721', '1', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('722', '2', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('723', '11', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('724', '7', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('725', '5', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('726', '7', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('727', '3', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('728', '2', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('729', '12', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('730', '6', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('731', '12', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('732', '2', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('733', '4', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('734', '13', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('735', '11', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('736', '3', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('737', '10', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('738', '13', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('739', '4', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('740', '2', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('741', '3', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('742', '4', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('743', '11', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('744', '4', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('745', '3', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('746', '3', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('747', '10', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('748', '3', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('749', '1', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('750', '1', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('751', '12', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('752', '10', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('753', '9', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('754', '7', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('755', '8', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('756', '11', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('757', '4', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('758', '12', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('759', '13', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('760', '6', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('761', '10', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('762', '11', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('763', '4', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('764', '10', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('765', '12', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('766', '10', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('767', '12', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('768', '11', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('769', '5', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('770', '13', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('771', '13', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('772', '1', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('773', '4', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('774', '4', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('775', '8', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('776', '11', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('777', '13', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('778', '3', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('779', '6', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('780', '3', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('781', '4', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('782', '8', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('783', '12', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('784', '10', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('785', '11', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('786', '4', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('787', '5', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('788', '7', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('789', '8', '5', '2');
INSERT INTO `dim_pginfo` VALUES ('790', '7', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('791', '11', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('792', '9', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('793', '12', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('794', '3', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('795', '11', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('796', '8', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('797', '9', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('798', '12', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('799', '8', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('800', '6', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('801', '13', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('802', '2', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('803', '8', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('804', '7', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('805', '11', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('806', '5', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('807', '1', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('808', '8', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('809', '3', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('810', '10', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('811', '12', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('812', '2', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('813', '6', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('814', '7', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('815', '12', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('816', '11', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('817', '13', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('818', '4', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('819', '5', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('820', '13', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('821', '8', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('822', '13', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('823', '6', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('824', '11', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('825', '3', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('826', '11', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('827', '13', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('828', '5', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('829', '2', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('830', '10', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('831', '1', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('832', '11', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('833', '9', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('834', '11', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('835', '10', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('836', '1', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('837', '5', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('838', '5', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('839', '8', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('840', '8', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('841', '5', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('842', '3', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('843', '9', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('844', '1', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('845', '2', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('846', '10', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('847', '4', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('848', '1', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('849', '7', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('850', '4', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('851', '9', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('852', '13', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('853', '7', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('854', '1', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('855', '5', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('856', '12', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('857', '10', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('858', '10', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('859', '5', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('860', '6', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('861', '12', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('862', '1', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('863', '7', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('864', '11', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('865', '3', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('866', '10', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('867', '5', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('868', '10', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('869', '1', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('870', '3', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('871', '6', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('872', '3', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('873', '9', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('874', '1', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('875', '6', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('876', '10', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('877', '9', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('878', '6', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('879', '9', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('880', '2', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('881', '6', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('882', '8', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('883', '11', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('884', '12', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('885', '11', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('886', '10', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('887', '6', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('888', '12', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('889', '13', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('890', '9', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('891', '12', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('892', '4', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('893', '9', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('894', '4', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('895', '4', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('896', '2', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('897', '1', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('898', '3', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('899', '5', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('900', '4', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('901', '5', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('902', '2', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('903', '7', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('904', '7', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('905', '13', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('906', '12', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('907', '10', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('908', '6', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('909', '7', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('910', '1', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('911', '8', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('912', '7', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('913', '3', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('914', '3', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('915', '11', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('916', '6', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('917', '3', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('918', '1', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('919', '5', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('920', '7', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('921', '12', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('922', '10', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('923', '6', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('924', '3', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('925', '8', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('926', '9', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('927', '11', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('928', '2', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('929', '1', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('930', '8', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('931', '6', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('932', '6', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('933', '4', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('934', '10', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('935', '10', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('936', '9', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('937', '9', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('938', '13', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('939', '8', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('940', '11', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('941', '8', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('942', '6', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('943', '2', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('944', '10', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('945', '8', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('946', '6', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('947', '3', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('948', '6', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('949', '12', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('950', '7', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('951', '9', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('952', '1', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('953', '7', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('954', '8', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('955', '7', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('956', '9', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('957', '3', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('958', '11', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('959', '7', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('960', '1', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('961', '6', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('962', '3', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('963', '13', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('964', '13', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('965', '3', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('966', '7', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('967', '1', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('968', '3', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('969', '9', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('970', '6', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('971', '12', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('972', '9', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('973', '5', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('974', '5', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('975', '5', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('976', '9', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('977', '12', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('978', '7', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('979', '4', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('980', '4', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('981', '7', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('982', '11', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('983', '2', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('984', '3', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('985', '3', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('986', '12', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('987', '4', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('988', '1', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('989', '7', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('990', '5', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('991', '4', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('992', '5', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('993', '3', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('994', '1', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('995', '13', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('996', '2', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('997', '8', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('998', '3', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('999', '13', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1000', '8', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('1001', '3', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1002', '1', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1003', '7', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('1004', '5', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('1005', '13', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('1006', '1', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('1007', '9', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1008', '5', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1009', '7', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1010', '11', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1011', '13', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('1012', '12', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('1013', '4', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1014', '8', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('1015', '2', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1016', '4', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1017', '1', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('1018', '6', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1019', '10', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('1020', '11', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('1021', '8', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('1022', '6', '19', '3');
INSERT INTO `dim_pginfo` VALUES ('1023', '8', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1024', '12', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('1025', '6', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('1026', '11', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('1027', '12', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('1028', '7', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1029', '13', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1030', '5', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('1031', '13', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1032', '7', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1033', '11', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('1034', '7', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('1035', '9', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('1036', '12', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1037', '3', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1038', '13', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('1039', '9', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1040', '2', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1041', '2', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('1042', '1', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('1043', '2', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('1044', '8', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1045', '12', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1046', '8', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('1047', '7', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1048', '12', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('1049', '9', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1050', '12', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('1051', '2', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1052', '4', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('1053', '10', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('1054', '13', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1055', '8', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1056', '10', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1057', '11', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1058', '6', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('1059', '6', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1060', '11', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1061', '13', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1062', '4', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1063', '5', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1064', '2', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('1065', '4', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('1066', '5', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('1067', '8', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1068', '12', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1069', '4', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1070', '3', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('1071', '6', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('1072', '6', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1073', '1', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1074', '11', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('1075', '13', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1076', '5', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1077', '12', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('1078', '5', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1079', '2', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('1080', '1', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1081', '9', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('1082', '10', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1083', '11', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1084', '3', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1085', '7', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1086', '3', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1087', '3', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1088', '4', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('1089', '1', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('1090', '10', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('1091', '8', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('1092', '9', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1093', '10', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1094', '6', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('1095', '3', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('1096', '11', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('1097', '13', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1098', '10', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1099', '8', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1100', '1', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('1101', '1', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1102', '11', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('1103', '9', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('1104', '11', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1105', '4', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('1106', '12', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('1107', '9', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1108', '2', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('1109', '12', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1110', '13', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1111', '11', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1112', '9', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1113', '7', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1114', '11', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1115', '4', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1116', '11', '4', '6');
INSERT INTO `dim_pginfo` VALUES ('1117', '5', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('1118', '3', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1119', '5', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('1120', '10', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('1121', '1', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1122', '1', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('1123', '6', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('1124', '4', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1125', '5', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1126', '8', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('1127', '8', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1128', '1', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1129', '7', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('1130', '8', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('1131', '10', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('1132', '6', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1133', '2', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('1134', '1', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('1135', '4', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('1136', '1', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1137', '5', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1138', '13', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1139', '6', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1140', '1', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('1141', '10', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1142', '5', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1143', '2', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1144', '10', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1145', '2', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1146', '13', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1147', '11', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1148', '10', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('1149', '5', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1150', '5', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('1151', '11', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('1152', '8', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('1153', '1', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1154', '11', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('1155', '3', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1156', '12', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('1157', '10', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1158', '2', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('1159', '8', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('1160', '10', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1161', '1', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1162', '3', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1163', '2', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('1164', '5', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1165', '3', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1166', '6', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('1167', '8', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('1168', '4', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1169', '9', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1170', '3', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('1171', '4', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1172', '11', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1173', '13', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('1174', '1', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('1175', '1', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('1176', '2', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1177', '2', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('1178', '10', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('1179', '5', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('1180', '11', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1181', '9', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1182', '12', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1183', '11', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1184', '11', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1185', '8', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1186', '1', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1187', '13', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1188', '2', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1189', '12', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1190', '12', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('1191', '1', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('1192', '6', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1193', '6', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1194', '1', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1195', '4', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('1196', '3', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('1197', '7', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('1198', '8', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1199', '4', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1200', '1', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('1201', '13', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1202', '4', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1203', '6', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1204', '12', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('1205', '2', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('1206', '1', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('1207', '7', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('1208', '4', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('1209', '4', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1210', '3', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1211', '3', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1212', '13', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1213', '10', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1214', '5', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1215', '8', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('1216', '8', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1217', '7', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1218', '7', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1219', '5', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1220', '2', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1221', '13', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1222', '12', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('1223', '8', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1224', '9', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1225', '12', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('1226', '4', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1227', '4', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1228', '6', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1229', '12', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1230', '5', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('1231', '9', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('1232', '10', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('1233', '3', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('1234', '13', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('1235', '5', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1236', '13', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('1237', '4', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1238', '13', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1239', '12', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1240', '4', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1241', '3', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1242', '6', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('1243', '3', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1244', '8', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1245', '11', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('1246', '11', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('1247', '3', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1248', '8', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('1249', '3', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1250', '1', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1251', '6', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1252', '6', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1253', '12', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('1254', '3', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('1255', '13', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('1256', '1', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('1257', '6', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1258', '4', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('1259', '12', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('1260', '10', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('1261', '6', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('1262', '1', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('1263', '8', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('1264', '2', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('1265', '8', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('1266', '10', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1267', '12', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('1268', '10', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1269', '2', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1270', '8', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1271', '1', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('1272', '5', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1273', '2', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1274', '11', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1275', '9', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1276', '1', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('1277', '10', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1278', '2', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1279', '8', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('1280', '10', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1281', '8', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('1282', '12', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1283', '12', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('1284', '5', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('1285', '9', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('1286', '9', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1287', '1', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('1288', '13', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('1289', '8', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1290', '3', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1291', '9', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('1292', '9', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1293', '7', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1294', '3', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1295', '13', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1296', '2', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('1297', '11', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1298', '1', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1299', '7', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1300', '11', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1301', '8', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('1302', '8', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('1303', '8', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1304', '11', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1305', '12', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('1306', '2', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('1307', '2', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1308', '13', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1309', '2', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1310', '7', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1311', '11', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('1312', '8', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('1313', '12', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('1314', '10', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1315', '1', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('1316', '8', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1317', '5', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('1318', '8', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('1319', '7', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('1320', '8', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('1321', '7', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1322', '13', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('1323', '13', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('1324', '4', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('1325', '2', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('1326', '13', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1327', '7', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1328', '5', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1329', '7', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1330', '5', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1331', '5', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1332', '12', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1333', '12', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('1334', '8', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('1335', '4', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1336', '6', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('1337', '8', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1338', '12', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1339', '2', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1340', '13', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1341', '12', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('1342', '6', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1343', '5', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1344', '7', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1345', '3', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('1346', '5', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('1347', '13', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('1348', '13', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1349', '11', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('1350', '10', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('1351', '2', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1352', '10', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('1353', '10', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('1354', '5', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1355', '4', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1356', '11', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('1357', '9', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('1358', '3', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1359', '4', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1360', '1', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1361', '12', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('1362', '11', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1363', '8', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('1364', '9', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1365', '9', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1366', '13', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('1367', '3', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1368', '7', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1369', '6', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('1370', '3', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('1371', '2', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('1372', '3', '5', '2');
INSERT INTO `dim_pginfo` VALUES ('1373', '4', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('1374', '7', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1375', '4', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1376', '9', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('1377', '2', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('1378', '12', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('1379', '13', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('1380', '3', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1381', '6', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('1382', '9', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('1383', '4', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1384', '1', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('1385', '8', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1386', '4', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('1387', '13', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('1388', '12', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1389', '9', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('1390', '12', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('1391', '2', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1392', '5', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('1393', '10', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('1394', '3', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('1395', '8', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('1396', '10', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('1397', '12', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1398', '10', '4', '6');
INSERT INTO `dim_pginfo` VALUES ('1399', '2', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('1400', '2', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('1401', '5', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1402', '4', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('1403', '3', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1404', '4', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1405', '6', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('1406', '9', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('1407', '11', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1408', '5', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('1409', '12', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1410', '2', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1411', '3', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('1412', '10', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1413', '4', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1414', '4', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('1415', '11', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('1416', '5', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('1417', '7', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('1418', '10', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1419', '8', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1420', '10', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('1421', '6', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1422', '11', '3', '5');
INSERT INTO `dim_pginfo` VALUES ('1423', '3', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('1424', '4', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('1425', '1', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1426', '2', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('1427', '11', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('1428', '8', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('1429', '2', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('1430', '12', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('1431', '9', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1432', '11', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('1433', '13', '20', '1');
INSERT INTO `dim_pginfo` VALUES ('1434', '2', '18', '2');
INSERT INTO `dim_pginfo` VALUES ('1435', '12', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('1436', '11', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('1437', '2', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1438', '4', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('1439', '13', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('1440', '8', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('1441', '5', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('1442', '2', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1443', '7', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('1444', '10', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('1445', '6', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1446', '12', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('1447', '12', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('1448', '2', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1449', '4', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1450', '5', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('1451', '13', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1452', '10', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('1453', '5', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1454', '4', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('1455', '11', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('1456', '13', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('1457', '12', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('1458', '13', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('1459', '1', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1460', '13', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('1461', '8', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('1462', '7', '10', '1');
INSERT INTO `dim_pginfo` VALUES ('1463', '11', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('1464', '9', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('1465', '12', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1466', '13', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1467', '4', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1468', '12', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('1469', '9', '19', '3');
INSERT INTO `dim_pginfo` VALUES ('1470', '7', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('1471', '13', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('1472', '6', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1473', '6', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('1474', '5', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('1475', '13', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1476', '3', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1477', '3', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1478', '12', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('1479', '3', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('1480', '5', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('1481', '11', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('1482', '6', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1483', '9', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1484', '10', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1485', '6', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1486', '6', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('1487', '12', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('1488', '2', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('1489', '5', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('1490', '3', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('1491', '6', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('1492', '2', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('1493', '13', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('1494', '8', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1495', '12', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1496', '5', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1497', '4', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1498', '8', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('1499', '10', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1500', '5', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('1501', '13', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1502', '11', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('1503', '7', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1504', '10', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('1505', '5', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('1506', '10', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1507', '13', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('1508', '8', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('1509', '4', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1510', '4', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1511', '3', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('1512', '9', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1513', '8', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('1514', '1', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1515', '7', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1516', '9', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1517', '6', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('1518', '13', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1519', '12', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1520', '13', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1521', '5', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1522', '8', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1523', '8', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('1524', '9', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1525', '8', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1526', '8', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('1527', '4', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1528', '13', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('1529', '8', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('1530', '1', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1531', '8', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('1532', '10', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1533', '9', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('1534', '3', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('1535', '9', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('1536', '6', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('1537', '10', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1538', '9', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('1539', '2', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('1540', '7', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1541', '4', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('1542', '5', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('1543', '1', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('1544', '6', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('1545', '2', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('1546', '8', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1547', '1', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1548', '9', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1549', '4', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1550', '1', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1551', '9', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1552', '4', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('1553', '1', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('1554', '13', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1555', '9', '11', '6');
INSERT INTO `dim_pginfo` VALUES ('1556', '7', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1557', '7', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1558', '1', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('1559', '3', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('1560', '11', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('1561', '10', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1562', '11', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('1563', '2', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1564', '10', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1565', '3', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('1566', '1', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1567', '13', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1568', '11', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1569', '4', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1570', '4', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1571', '8', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('1572', '10', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1573', '6', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('1574', '9', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('1575', '1', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1576', '4', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('1577', '13', '3', '2');
INSERT INTO `dim_pginfo` VALUES ('1578', '13', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('1579', '4', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('1580', '1', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('1581', '3', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1582', '11', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1583', '5', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('1584', '13', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1585', '1', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1586', '6', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('1587', '8', '19', '3');
INSERT INTO `dim_pginfo` VALUES ('1588', '13', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('1589', '11', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1590', '8', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('1591', '8', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1592', '7', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1593', '4', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1594', '6', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('1595', '12', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('1596', '4', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('1597', '5', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('1598', '12', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('1599', '5', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('1600', '3', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('1601', '6', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1602', '4', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('1603', '7', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1604', '3', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1605', '1', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('1606', '10', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1607', '2', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('1608', '1', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1609', '6', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('1610', '8', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1611', '5', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1612', '1', '5', '2');
INSERT INTO `dim_pginfo` VALUES ('1613', '3', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1614', '13', '4', '5');
INSERT INTO `dim_pginfo` VALUES ('1615', '10', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('1616', '3', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('1617', '1', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1618', '4', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('1619', '8', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1620', '13', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1621', '12', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('1622', '9', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('1623', '6', '2', '3');
INSERT INTO `dim_pginfo` VALUES ('1624', '5', '20', '5');
INSERT INTO `dim_pginfo` VALUES ('1625', '9', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1626', '12', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1627', '10', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1628', '12', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('1629', '11', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1630', '5', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('1631', '13', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('1632', '3', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('1633', '7', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1634', '1', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('1635', '10', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1636', '3', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1637', '7', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1638', '7', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('1639', '7', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1640', '4', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('1641', '11', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1642', '8', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('1643', '7', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('1644', '9', '7', '6');
INSERT INTO `dim_pginfo` VALUES ('1645', '6', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1646', '12', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('1647', '9', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('1648', '7', '8', '2');
INSERT INTO `dim_pginfo` VALUES ('1649', '2', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1650', '9', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1651', '13', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1652', '10', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1653', '2', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1654', '13', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1655', '6', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1656', '6', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1657', '7', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1658', '5', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('1659', '11', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1660', '3', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1661', '3', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('1662', '11', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('1663', '3', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('1664', '2', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('1665', '13', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('1666', '4', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1667', '9', '3', '7');
INSERT INTO `dim_pginfo` VALUES ('1668', '2', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('1669', '7', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1670', '9', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1671', '7', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('1672', '7', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('1673', '12', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1674', '10', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('1675', '6', '19', '3');
INSERT INTO `dim_pginfo` VALUES ('1676', '2', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('1677', '2', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1678', '11', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1679', '7', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1680', '12', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1681', '10', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1682', '13', '16', '5');
INSERT INTO `dim_pginfo` VALUES ('1683', '1', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1684', '6', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('1685', '3', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1686', '12', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1687', '13', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('1688', '8', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('1689', '9', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1690', '12', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('1691', '2', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('1692', '3', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('1693', '2', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1694', '12', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('1695', '12', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('1696', '10', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('1697', '7', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1698', '13', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('1699', '12', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1700', '9', '21', '3');
INSERT INTO `dim_pginfo` VALUES ('1701', '7', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('1702', '5', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1703', '9', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1704', '7', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1705', '2', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1706', '5', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('1707', '13', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1708', '1', '2', '4');
INSERT INTO `dim_pginfo` VALUES ('1709', '5', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1710', '10', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('1711', '1', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1712', '1', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('1713', '7', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('1714', '1', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('1715', '7', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('1716', '7', '7', '7');
INSERT INTO `dim_pginfo` VALUES ('1717', '5', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('1718', '12', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('1719', '7', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('1720', '2', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('1721', '6', '13', '2');
INSERT INTO `dim_pginfo` VALUES ('1722', '2', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('1723', '3', '6', '7');
INSERT INTO `dim_pginfo` VALUES ('1724', '9', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1725', '2', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1726', '3', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1727', '11', '15', '1');
INSERT INTO `dim_pginfo` VALUES ('1728', '10', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1729', '7', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('1730', '11', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1731', '12', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('1732', '12', '1', '2');
INSERT INTO `dim_pginfo` VALUES ('1733', '1', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1734', '8', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1735', '13', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('1736', '2', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('1737', '2', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1738', '10', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1739', '6', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('1740', '13', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('1741', '8', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('1742', '10', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('1743', '10', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1744', '11', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1745', '1', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('1746', '13', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('1747', '11', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1748', '5', '3', '3');
INSERT INTO `dim_pginfo` VALUES ('1749', '5', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('1750', '12', '8', '6');
INSERT INTO `dim_pginfo` VALUES ('1751', '4', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1752', '11', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1753', '6', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1754', '3', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1755', '13', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1756', '10', '1', '1');
INSERT INTO `dim_pginfo` VALUES ('1757', '10', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1758', '3', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1759', '2', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('1760', '3', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1761', '13', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('1762', '1', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1763', '4', '1', '4');
INSERT INTO `dim_pginfo` VALUES ('1764', '10', '12', '5');
INSERT INTO `dim_pginfo` VALUES ('1765', '1', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1766', '13', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1767', '8', '18', '6');
INSERT INTO `dim_pginfo` VALUES ('1768', '1', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1769', '5', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1770', '3', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('1771', '8', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('1772', '13', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('1773', '13', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('1774', '5', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1775', '5', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1776', '2', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1777', '10', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1778', '12', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('1779', '4', '3', '4');
INSERT INTO `dim_pginfo` VALUES ('1780', '5', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1781', '7', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1782', '5', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1783', '2', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('1784', '5', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1785', '6', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('1786', '13', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1787', '12', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1788', '7', '11', '7');
INSERT INTO `dim_pginfo` VALUES ('1789', '5', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1790', '11', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1791', '11', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('1792', '5', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1793', '13', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1794', '7', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1795', '3', '9', '3');
INSERT INTO `dim_pginfo` VALUES ('1796', '4', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1797', '12', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('1798', '4', '20', '6');
INSERT INTO `dim_pginfo` VALUES ('1799', '1', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1800', '4', '8', '3');
INSERT INTO `dim_pginfo` VALUES ('1801', '2', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1802', '8', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('1803', '6', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1804', '10', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1805', '4', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1806', '3', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1807', '8', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('1808', '8', '5', '6');
INSERT INTO `dim_pginfo` VALUES ('1809', '3', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1810', '2', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1811', '10', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1812', '7', '19', '6');
INSERT INTO `dim_pginfo` VALUES ('1813', '1', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1814', '7', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1815', '1', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('1816', '11', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1817', '7', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('1818', '7', '19', '5');
INSERT INTO `dim_pginfo` VALUES ('1819', '8', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1820', '7', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1821', '1', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1822', '7', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('1823', '8', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('1824', '8', '7', '5');
INSERT INTO `dim_pginfo` VALUES ('1825', '4', '14', '7');
INSERT INTO `dim_pginfo` VALUES ('1826', '2', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('1827', '12', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1828', '5', '1', '5');
INSERT INTO `dim_pginfo` VALUES ('1829', '8', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1830', '8', '10', '2');
INSERT INTO `dim_pginfo` VALUES ('1831', '6', '16', '3');
INSERT INTO `dim_pginfo` VALUES ('1832', '6', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('1833', '8', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('1834', '9', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1835', '11', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1836', '1', '2', '5');
INSERT INTO `dim_pginfo` VALUES ('1837', '12', '8', '5');
INSERT INTO `dim_pginfo` VALUES ('1838', '3', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('1839', '11', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('1840', '9', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1841', '5', '17', '1');
INSERT INTO `dim_pginfo` VALUES ('1842', '6', '14', '3');
INSERT INTO `dim_pginfo` VALUES ('1843', '5', '15', '2');
INSERT INTO `dim_pginfo` VALUES ('1844', '8', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1845', '12', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('1846', '7', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1847', '6', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('1848', '6', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1849', '11', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1850', '4', '10', '6');
INSERT INTO `dim_pginfo` VALUES ('1851', '8', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1852', '9', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('1853', '11', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1854', '5', '4', '1');
INSERT INTO `dim_pginfo` VALUES ('1855', '10', '21', '1');
INSERT INTO `dim_pginfo` VALUES ('1856', '1', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('1857', '10', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1858', '10', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1859', '4', '1', '6');
INSERT INTO `dim_pginfo` VALUES ('1860', '11', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('1861', '7', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1862', '3', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('1863', '9', '21', '5');
INSERT INTO `dim_pginfo` VALUES ('1864', '2', '13', '3');
INSERT INTO `dim_pginfo` VALUES ('1865', '8', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('1866', '3', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1867', '13', '11', '4');
INSERT INTO `dim_pginfo` VALUES ('1868', '9', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1869', '9', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('1870', '13', '8', '4');
INSERT INTO `dim_pginfo` VALUES ('1871', '2', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('1872', '6', '7', '3');
INSERT INTO `dim_pginfo` VALUES ('1873', '8', '14', '5');
INSERT INTO `dim_pginfo` VALUES ('1874', '11', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1875', '5', '10', '5');
INSERT INTO `dim_pginfo` VALUES ('1876', '8', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('1877', '8', '2', '1');
INSERT INTO `dim_pginfo` VALUES ('1878', '6', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('1879', '4', '2', '7');
INSERT INTO `dim_pginfo` VALUES ('1880', '7', '12', '4');
INSERT INTO `dim_pginfo` VALUES ('1881', '9', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('1882', '2', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1883', '3', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1884', '8', '14', '4');
INSERT INTO `dim_pginfo` VALUES ('1885', '12', '4', '4');
INSERT INTO `dim_pginfo` VALUES ('1886', '3', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1887', '9', '15', '3');
INSERT INTO `dim_pginfo` VALUES ('1888', '9', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('1889', '3', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1890', '5', '18', '3');
INSERT INTO `dim_pginfo` VALUES ('1891', '2', '2', '6');
INSERT INTO `dim_pginfo` VALUES ('1892', '9', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('1893', '8', '11', '3');
INSERT INTO `dim_pginfo` VALUES ('1894', '8', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1895', '4', '16', '4');
INSERT INTO `dim_pginfo` VALUES ('1896', '12', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1897', '10', '9', '5');
INSERT INTO `dim_pginfo` VALUES ('1898', '11', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1899', '3', '8', '1');
INSERT INTO `dim_pginfo` VALUES ('1900', '10', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('1901', '7', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1902', '4', '9', '7');
INSERT INTO `dim_pginfo` VALUES ('1903', '5', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1904', '4', '3', '6');
INSERT INTO `dim_pginfo` VALUES ('1905', '9', '17', '3');
INSERT INTO `dim_pginfo` VALUES ('1906', '12', '14', '2');
INSERT INTO `dim_pginfo` VALUES ('1907', '5', '18', '7');
INSERT INTO `dim_pginfo` VALUES ('1908', '11', '3', '1');
INSERT INTO `dim_pginfo` VALUES ('1909', '10', '4', '2');
INSERT INTO `dim_pginfo` VALUES ('1910', '2', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1911', '3', '13', '1');
INSERT INTO `dim_pginfo` VALUES ('1912', '5', '15', '4');
INSERT INTO `dim_pginfo` VALUES ('1913', '10', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1914', '6', '6', '4');
INSERT INTO `dim_pginfo` VALUES ('1915', '8', '6', '3');
INSERT INTO `dim_pginfo` VALUES ('1916', '11', '14', '6');
INSERT INTO `dim_pginfo` VALUES ('1917', '11', '6', '2');
INSERT INTO `dim_pginfo` VALUES ('1918', '10', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1919', '12', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1920', '13', '17', '6');
INSERT INTO `dim_pginfo` VALUES ('1921', '10', '14', '1');
INSERT INTO `dim_pginfo` VALUES ('1922', '10', '18', '4');
INSERT INTO `dim_pginfo` VALUES ('1923', '10', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('1924', '11', '6', '1');
INSERT INTO `dim_pginfo` VALUES ('1925', '1', '18', '1');
INSERT INTO `dim_pginfo` VALUES ('1926', '5', '9', '4');
INSERT INTO `dim_pginfo` VALUES ('1927', '4', '21', '4');
INSERT INTO `dim_pginfo` VALUES ('1928', '1', '18', '5');
INSERT INTO `dim_pginfo` VALUES ('1929', '4', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1930', '7', '13', '6');
INSERT INTO `dim_pginfo` VALUES ('1931', '10', '5', '3');
INSERT INTO `dim_pginfo` VALUES ('1932', '2', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1933', '11', '16', '1');
INSERT INTO `dim_pginfo` VALUES ('1934', '5', '10', '4');
INSERT INTO `dim_pginfo` VALUES ('1935', '10', '12', '7');
INSERT INTO `dim_pginfo` VALUES ('1936', '10', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('1937', '9', '21', '7');
INSERT INTO `dim_pginfo` VALUES ('1938', '6', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1939', '7', '13', '5');
INSERT INTO `dim_pginfo` VALUES ('1940', '1', '17', '4');
INSERT INTO `dim_pginfo` VALUES ('1941', '13', '20', '3');
INSERT INTO `dim_pginfo` VALUES ('1942', '9', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('1943', '3', '2', '2');
INSERT INTO `dim_pginfo` VALUES ('1944', '5', '9', '2');
INSERT INTO `dim_pginfo` VALUES ('1945', '3', '5', '4');
INSERT INTO `dim_pginfo` VALUES ('1946', '4', '6', '6');
INSERT INTO `dim_pginfo` VALUES ('1947', '4', '12', '6');
INSERT INTO `dim_pginfo` VALUES ('1948', '10', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('1949', '12', '19', '2');
INSERT INTO `dim_pginfo` VALUES ('1950', '7', '4', '6');
INSERT INTO `dim_pginfo` VALUES ('1951', '13', '20', '7');
INSERT INTO `dim_pginfo` VALUES ('1952', '3', '6', '5');
INSERT INTO `dim_pginfo` VALUES ('1953', '9', '19', '1');
INSERT INTO `dim_pginfo` VALUES ('1954', '4', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1955', '11', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('1956', '12', '5', '5');
INSERT INTO `dim_pginfo` VALUES ('1957', '1', '1', '3');
INSERT INTO `dim_pginfo` VALUES ('1958', '11', '11', '1');
INSERT INTO `dim_pginfo` VALUES ('1959', '12', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('1960', '12', '5', '1');
INSERT INTO `dim_pginfo` VALUES ('1961', '12', '4', '3');
INSERT INTO `dim_pginfo` VALUES ('1962', '11', '17', '2');
INSERT INTO `dim_pginfo` VALUES ('1963', '13', '10', '7');
INSERT INTO `dim_pginfo` VALUES ('1964', '12', '9', '6');
INSERT INTO `dim_pginfo` VALUES ('1965', '2', '12', '1');
INSERT INTO `dim_pginfo` VALUES ('1966', '9', '16', '2');
INSERT INTO `dim_pginfo` VALUES ('1967', '7', '12', '2');
INSERT INTO `dim_pginfo` VALUES ('1968', '4', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('1969', '12', '16', '7');
INSERT INTO `dim_pginfo` VALUES ('1970', '11', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('1971', '12', '4', '7');
INSERT INTO `dim_pginfo` VALUES ('1972', '13', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('1973', '10', '15', '6');
INSERT INTO `dim_pginfo` VALUES ('1974', '7', '10', '3');
INSERT INTO `dim_pginfo` VALUES ('1975', '12', '11', '2');
INSERT INTO `dim_pginfo` VALUES ('1976', '7', '7', '1');
INSERT INTO `dim_pginfo` VALUES ('1977', '11', '17', '7');
INSERT INTO `dim_pginfo` VALUES ('1978', '13', '11', '5');
INSERT INTO `dim_pginfo` VALUES ('1979', '9', '15', '7');
INSERT INTO `dim_pginfo` VALUES ('1980', '10', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1981', '5', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1982', '8', '8', '7');
INSERT INTO `dim_pginfo` VALUES ('1983', '13', '21', '6');
INSERT INTO `dim_pginfo` VALUES ('1984', '8', '19', '4');
INSERT INTO `dim_pginfo` VALUES ('1985', '13', '1', '7');
INSERT INTO `dim_pginfo` VALUES ('1986', '12', '12', '3');
INSERT INTO `dim_pginfo` VALUES ('1987', '6', '21', '2');
INSERT INTO `dim_pginfo` VALUES ('1988', '6', '5', '7');
INSERT INTO `dim_pginfo` VALUES ('1989', '13', '16', '6');
INSERT INTO `dim_pginfo` VALUES ('1990', '10', '7', '2');
INSERT INTO `dim_pginfo` VALUES ('1991', '1', '13', '7');
INSERT INTO `dim_pginfo` VALUES ('1992', '4', '13', '4');
INSERT INTO `dim_pginfo` VALUES ('1993', '12', '9', '1');
INSERT INTO `dim_pginfo` VALUES ('1994', '1', '15', '5');
INSERT INTO `dim_pginfo` VALUES ('1995', '8', '19', '7');
INSERT INTO `dim_pginfo` VALUES ('1996', '13', '20', '2');
INSERT INTO `dim_pginfo` VALUES ('1997', '6', '7', '4');
INSERT INTO `dim_pginfo` VALUES ('1998', '7', '20', '4');
INSERT INTO `dim_pginfo` VALUES ('1999', '6', '17', '5');
INSERT INTO `dim_pginfo` VALUES ('2000', '8', '4', '7');

-- ----------------------------
-- Table structure for dim_pgtype
-- ----------------------------
DROP TABLE IF EXISTS `dim_pgtype`;
CREATE TABLE `dim_pgtype` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `typename` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dim_pgtype
-- ----------------------------
INSERT INTO `dim_pgtype` VALUES ('1', 'ch_home');
INSERT INTO `dim_pgtype` VALUES ('2', 'ct_home');
INSERT INTO `dim_pgtype` VALUES ('3', 'lm_home');
INSERT INTO `dim_pgtype` VALUES ('4', 'sc_result');
INSERT INTO `dim_pgtype` VALUES ('5', 'pm_home');
INSERT INTO `dim_pgtype` VALUES ('6', 'pd_dtl');
INSERT INTO `dim_pgtype` VALUES ('7', 'idx');

-- ----------------------------
-- Table structure for dim_pindao
-- ----------------------------
DROP TABLE IF EXISTS `dim_pindao`;
CREATE TABLE `dim_pindao` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dim_pindao
-- ----------------------------
INSERT INTO `dim_pindao` VALUES ('1', 'pd_aaa');
INSERT INTO `dim_pindao` VALUES ('2', 'pd_bbb');
INSERT INTO `dim_pindao` VALUES ('3', 'pd_ccc');
INSERT INTO `dim_pindao` VALUES ('4', 'pd_ddd');
INSERT INTO `dim_pindao` VALUES ('5', 'pd_eee');
INSERT INTO `dim_pindao` VALUES ('6', 'pd_fff');
INSERT INTO `dim_pindao` VALUES ('7', 'pd_ggg');
INSERT INTO `dim_pindao` VALUES ('8', 'pd_hhh');
INSERT INTO `dim_pindao` VALUES ('9', 'pd_jjj');
INSERT INTO `dim_pindao` VALUES ('10', 'pd_kkk');
INSERT INTO `dim_pindao` VALUES ('11', 'pd_ooo');
INSERT INTO `dim_pindao` VALUES ('12', 'pd_ppp');
INSERT INTO `dim_pindao` VALUES ('13', 'pd_uuu');

-- ----------------------------
-- Table structure for dim_promotion_loc
-- ----------------------------
DROP TABLE IF EXISTS `dim_promotion_loc`;
CREATE TABLE `dim_promotion_loc` (
  `id` int(11) NOT NULL,
  `loc_name` varchar(255) DEFAULT NULL,
  `loc_type` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dim_promotion_loc
-- ----------------------------

-- ----------------------------
-- Table structure for huodonginfo
-- ----------------------------
DROP TABLE IF EXISTS `huodonginfo`;
CREATE TABLE `huodonginfo` (
  `id` int(11) NOT NULL,
  `huodongName` varchar(255) DEFAULT NULL,
  `startTime` varchar(255) DEFAULT NULL,
  `endTime` varchar(255) DEFAULT NULL,
  `productId` int(11) DEFAULT NULL,
  `merchartId` int(11) DEFAULT NULL,
  `shopid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of huodonginfo
-- ----------------------------
INSERT INTO `huodonginfo` VALUES ('1', '中秋大减价活动', '2020-06-21 11:12:05', '2020-06-21 11:12:05', '1', '2', '1');
INSERT INTO `huodonginfo` VALUES ('2', '国庆大优惠活动', '2020-06-21 11:12:39', '2020-06-21 11:12:39', '3', '2', '1');

-- ----------------------------
-- Table structure for merchart
-- ----------------------------
DROP TABLE IF EXISTS `merchart`;
CREATE TABLE `merchart` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `merchartName` varchar(255) DEFAULT NULL,
  `merchartArea` varchar(255) DEFAULT NULL,
  `shopnum` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of merchart
-- ----------------------------

-- ----------------------------
-- Table structure for miaoshainfo
-- ----------------------------
DROP TABLE IF EXISTS `miaoshainfo`;
CREATE TABLE `miaoshainfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `miaoshaName` varchar(255) DEFAULT NULL,
  `startTime` varchar(255) DEFAULT NULL,
  `endTime` varchar(255) DEFAULT NULL,
  `originPrice` decimal(10,2) DEFAULT NULL,
  `miaoshaPrice` decimal(10,2) DEFAULT NULL,
  `productId` int(11) DEFAULT NULL,
  `shopId` int(11) DEFAULT NULL,
  `merchantId` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of miaoshainfo
-- ----------------------------
INSERT INTO `miaoshainfo` VALUES ('1', '国庆秒杀活动', '2020-06-10', '2020-06-22', '15.00', '7.50', '3', '1', '2');
INSERT INTO `miaoshainfo` VALUES ('2', '中秋秒杀活动', '2020-06-15', '2020-06-25', '20.00', '22.00', '1', '1', '1');

-- ----------------------------
-- Table structure for product
-- ----------------------------
DROP TABLE IF EXISTS `product`;
CREATE TABLE `product` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `productName` varchar(255) DEFAULT NULL,
  `productTypeId` int(11) DEFAULT NULL,
  `huodongPrice` decimal(10,2) DEFAULT NULL,
  `originPrice` decimal(10,2) DEFAULT NULL,
  `shopid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of product
-- ----------------------------

-- ----------------------------
-- Table structure for product_detail
-- ----------------------------
DROP TABLE IF EXISTS `product_detail`;
CREATE TABLE `product_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `productid` int(11) DEFAULT NULL,
  `place` varchar(255) DEFAULT NULL,
  `brand` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of product_detail
-- ----------------------------

-- ----------------------------
-- Table structure for product_type
-- ----------------------------
DROP TABLE IF EXISTS `product_type`;
CREATE TABLE `product_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `productTypeName` varchar(255) DEFAULT NULL,
  `productTypeleave` varchar(255) DEFAULT NULL,
  `parentId` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of product_type
-- ----------------------------

-- ----------------------------
-- Table structure for shopinfo
-- ----------------------------
DROP TABLE IF EXISTS `shopinfo`;
CREATE TABLE `shopinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `shopname` varchar(255) DEFAULT NULL,
  `merchartId` int(11) DEFAULT NULL,
  `shopdesc` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of shopinfo
-- ----------------------------

-- ----------------------------
-- Table structure for tuangouinfo
-- ----------------------------
DROP TABLE IF EXISTS `tuangouinfo`;
CREATE TABLE `tuangouinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tuangouName` varchar(255) DEFAULT NULL,
  `tuangouTotalNum` int(11) DEFAULT NULL,
  `tuangouNum` int(11) DEFAULT NULL,
  `startTime` varchar(255) DEFAULT NULL,
  `endTime` varchar(255) DEFAULT NULL,
  `merchartId` int(11) DEFAULT NULL,
  `shopId` int(11) DEFAULT NULL,
  `productId` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of tuangouinfo
-- ----------------------------

-- ----------------------------
-- Table structure for user_info
-- ----------------------------
DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `account` varchar(255) DEFAULT NULL,
  `gender` int(20) DEFAULT NULL,
  `province` varchar(255) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  `birthday` varchar(255) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `phone` varchar(255) DEFAULT NULL,
  `vip_level` varchar(255) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=51 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of user_info
-- ----------------------------
INSERT INTO `user_info` VALUES ('1', 'KFM5lp', '1', '安徽', '池州', '1971-02-20', null, '13957100178', '4', 'sSSxvR9@7cbG.com');
INSERT INTO `user_info` VALUES ('2', 'wWRAho9y', '1', '辽宁', '营口', '2001-05-23', null, '13371508285', '5', 'SOgrsF@0zQXD.com');
INSERT INTO `user_info` VALUES ('3', '46SEkQa', '0', '天津', '天津', '1994-01-27', null, '15889676862', '1', 'cxKCFu6@0fbv.com');
INSERT INTO `user_info` VALUES ('4', '3Xu3g2Ak', '1', '贵州', '黔西南布依族苗族自治州', '1972-10-01', null, '13893432836', '5', 'Us3SQr@uz2.com');
INSERT INTO `user_info` VALUES ('5', 'E4KnRZ', '0', '海南', '儋州', '1984-05-13', null, '13582192525', '2', 'QcrK@007.com');
INSERT INTO `user_info` VALUES ('6', 'v8S9H', '0', '青海', '玉树藏族自治州', '1988-01-06', null, '13673831466', '5', 'ZP8Xg@VllPG.com');
INSERT INTO `user_info` VALUES ('7', 'xRyxN063S', '0', '内蒙古', '呼伦贝尔', '1978-12-19', null, '15852641052', '2', '0ZqZuhc@hZlV.com');
INSERT INTO `user_info` VALUES ('8', '5Cigc6Gj', '1', '香港', '香港', '1985-09-21', null, '18801078197', '5', 'K1HeT5@Gtudm.com');
INSERT INTO `user_info` VALUES ('9', 'FY4fR829', '1', '安徽', '黄山', '2001-02-21', null, '13934233346', '1', 'r83@Ebwo.com');
INSERT INTO `user_info` VALUES ('10', 'GpcBJas', '1', '内蒙古', '阿拉善盟', '2002-03-12', null, '18901284133', '3', 'veq@rz.com');
INSERT INTO `user_info` VALUES ('11', 'xq1Nlaaop', '1', '甘肃', '天水', '1995-03-07', null, '15885936120', '3', 'iuv@LkEU.com');
INSERT INTO `user_info` VALUES ('12', 'e4EDG6', '1', '内蒙古', '包头', '1981-01-07', null, '17272911217', '4', 'QTkUF@VW.com');
INSERT INTO `user_info` VALUES ('13', 'MtpZ2lV', '1', '山西', '长治', '1979-07-23', null, '17278402517', '4', '5sg@hDq.com');
INSERT INTO `user_info` VALUES ('14', 'OojqS36Vk', '0', '宁夏', '固原', '1982-06-07', null, '18895436308', '3', 'ExB8ACo@HVLo.com');
INSERT INTO `user_info` VALUES ('15', 'kcK3At', '1', '香港', '香港', '1985-04-21', null, '13956991615', '1', 'PcQ@10w.com');
INSERT INTO `user_info` VALUES ('16', '7rjmtsp', '1', '云南', '普洱', '1998-07-27', null, '17608913163', '1', 'tAyOs@HZ.com');
INSERT INTO `user_info` VALUES ('17', 'zZ1Qp9SZ4', '1', '澳门', '澳门', '1996-02-13', null, '13654410667', '4', 'Xi2E@Qb.com');
INSERT INTO `user_info` VALUES ('18', 'RgXJG', '1', '四川', '乐山', '1978-09-11', null, '13894217821', '5', 'ADXiIDQ@3rp5O.com');
INSERT INTO `user_info` VALUES ('19', 'OkiWCC5E', '0', '湖南', '湘西土家族苗族自治州', '1989-11-19', null, '13325814647', '4', 'iVGay6J@TUR6k.com');
INSERT INTO `user_info` VALUES ('20', 'ufM1CsM', '1', '山西', '忻州', '1985-09-23', null, '13941603288', '1', 'XxwyPd@BESm.com');
INSERT INTO `user_info` VALUES ('21', 'WCF4fdEek', '1', '青海', '玉树藏族自治州', '1992-03-21', null, '13759649093', '5', '61T7J@u7EK.com');
INSERT INTO `user_info` VALUES ('22', 'H4Ah5ryT', '1', '浙江', '温州', '1996-11-08', null, '17265589726', '3', 'TuWb@kM.com');
INSERT INTO `user_info` VALUES ('23', 'iylchCu', '0', '内蒙古', '包头', '1970-12-08', null, '15984822067', '4', 'ZpPy0QT@Bp.com');
INSERT INTO `user_info` VALUES ('24', 'YJTdm7', '1', '香港', '香港', '1975-06-04', null, '18943329211', '2', 'rp4GIf@C4.com');
INSERT INTO `user_info` VALUES ('25', 'WP8p', '0', '湖南', '常德', '2003-02-06', null, '17697678176', '4', 'dzyheI0@Y9.com');
INSERT INTO `user_info` VALUES ('26', 'DxL36Dom', '1', '甘肃', '酒泉', '1982-10-22', null, '17283299828', '3', '6qRQrVq@uKL.com');
INSERT INTO `user_info` VALUES ('27', 'csKUd83', '1', '云南', '曲靖', '2003-04-15', null, '13758543098', '2', 'yleHs@NOgbx.com');
INSERT INTO `user_info` VALUES ('28', '7bib0', '0', '内蒙古', '呼伦贝尔', '1979-08-23', null, '18817100392', '2', 'Prt@IHBBc.com');
INSERT INTO `user_info` VALUES ('29', 'RVZLcN', '0', '山东', '烟台', '1986-01-04', null, '13364684818', '4', 'sbcLQw@2uNic.com');
INSERT INTO `user_info` VALUES ('30', 'kAgvPSDn', '0', '山西', '运城', '1989-05-24', null, '13520223888', '5', 'PXaNRsw@sxfD4.com');
INSERT INTO `user_info` VALUES ('31', 'ibWcO', '1', '黑龙江', '齐齐哈尔', '1977-08-02', null, '13390776502', '1', 'dUiD9@dIK.com');
INSERT INTO `user_info` VALUES ('32', 'junenLh7P', '1', '青海', '海西蒙古族藏族自治州', '1971-09-05', null, '13776201016', '2', 'SgD@7nPz8.com');
INSERT INTO `user_info` VALUES ('33', 'YGf8LaQQ', '1', '吉林', '四平', '1971-02-27', null, '17616570339', '2', '0UXOL@sREe.com');
INSERT INTO `user_info` VALUES ('34', 'hqRAk6', '1', '江苏', '宿迁', '1997-01-13', null, '13517219481', '3', '6dck@Q005.com');
INSERT INTO `user_info` VALUES ('35', 'l4d9TM', '1', '北京', '北京', '2002-03-09', null, '13398878751', '2', 'er0fH@QB.com');
INSERT INTO `user_info` VALUES ('36', 'A0PxIXi', '0', '陕西', '西安', '1999-07-05', null, '13876578713', '5', '8MF@TUr2.com');
INSERT INTO `user_info` VALUES ('37', 'KD0oI', '0', '贵州', '遵义', '1973-08-03', null, '13530841344', '2', 'ZEU7@VHC.com');
INSERT INTO `user_info` VALUES ('38', 'fdlc8TRO', '0', '广东', '汕头', '1991-07-20', null, '17631781685', '5', 'T7uvZr@p9dE.com');
INSERT INTO `user_info` VALUES ('39', 'Fu46wfD4', '1', '青海', '黄南藏族自治州', '1981-05-03', null, '15832179892', '3', 'Lbjh7iA@3l80.com');
INSERT INTO `user_info` VALUES ('40', 'XzhkkBg', '1', '江苏', '连云港', '2001-01-11', null, '13561619263', '2', 'b0b9R@hri.com');
INSERT INTO `user_info` VALUES ('41', 'AwF1S', '0', '江苏', '泰州', '1977-08-10', null, '17247489868', '2', 'Uz4@Dsw.com');
INSERT INTO `user_info` VALUES ('42', 'eXqw', '1', '辽宁', '盘锦', '1985-12-15', null, '13639571259', '5', '1gv@Jkb0L.com');
INSERT INTO `user_info` VALUES ('43', '7UYT1B', '1', '江西', '景德镇', '1971-04-10', null, '13524051279', '3', 'EUNFYMC@kJGqt.com');
INSERT INTO `user_info` VALUES ('44', 'NB95', '1', '辽宁', '阜新', '1978-12-20', null, '13582206709', '5', 'Nvvp5bL@22fM.com');
INSERT INTO `user_info` VALUES ('45', '8jjf', '0', '湖北', '仙桃', '1982-12-25', null, '13733898265', '4', 'ZNOpQx@lpFPu.com');
INSERT INTO `user_info` VALUES ('46', 'QYiH', '1', '海南', '陵水黎族自治县', '1992-12-28', null, '13837153840', '5', 'iToT4@iBXn.com');
INSERT INTO `user_info` VALUES ('47', 'yITQmE', '1', '四川', '乐山', '2007-10-02', null, '18862049512', '3', 'Vg4qma@7c.com');
INSERT INTO `user_info` VALUES ('48', 'B7QH', '0', '四川', '泸州', '2005-04-08', null, '13719035172', '2', 'nCvT3@Sf.com');
INSERT INTO `user_info` VALUES ('49', 'Vz54E9Ya', '1', '贵州', '铜仁地区', '1971-11-09', null, '13378962883', '2', 'JHA13V@nIo.com');
INSERT INTO `user_info` VALUES ('50', 'q1OTH3G', '0', '贵州', '铜仁地区', '1987-05-17', null, '13900001703', '1', 'P1sgzFC@zar.com');
