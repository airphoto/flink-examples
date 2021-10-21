async_io 异步io：
使用MySQL作为缓存，异步获取数据

数据：

CREATE TABLE `t_category` (
  `id` int(11) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `t_category` VALUES ('1', '手机'),('2', '电脑'),('3', '服装'),('4', '化妆品'),('5', '食品');

AsyncFunction1：使用的是Java-vertx中提供的异步客户端实现的异步ui