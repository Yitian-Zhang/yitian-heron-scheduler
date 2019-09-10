
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for tb_cpu_load
-- ----------------------------
DROP TABLE IF EXISTS `tb_cpu_load`;
CREATE TABLE `tb_cpu_load` (
  `topology_id` varchar(255) DEFAULT NULL,
  `begin_task` int(255) DEFAULT NULL,
  `end_task` int(255) DEFAULT NULL,
  `load` bigint(255) DEFAULT NULL,
  `node` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for tb_node
-- ----------------------------
DROP TABLE IF EXISTS `tb_node`;
CREATE TABLE `tb_node` (
  `name` varchar(255) DEFAULT NULL,
  `capacity` bigint(255) DEFAULT NULL,
  `cores` int(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for tb_traffic
-- ----------------------------
DROP TABLE IF EXISTS `tb_traffic`;
CREATE TABLE `tb_traffic` (
  `topology_id` varchar(255) DEFAULT NULL,
  `source_task` int(255) DEFAULT NULL,
  `destination_task` int(255) DEFAULT NULL,
  `traffic` int(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
