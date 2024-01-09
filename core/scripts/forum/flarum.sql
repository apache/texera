-- MySQL dump 10.13  Distrib 8.2.0, for macos13 (arm64)
--
-- Host: 127.0.0.1    Database: flarum
-- ------------------------------------------------------
-- Server version	8.0.35-0ubuntu0.20.04.1

CREATE SCHEMA IF NOT EXISTS `flarum`;
USE `flarum`;

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `access_tokens`
--

DROP TABLE IF EXISTS `access_tokens`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `access_tokens` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `token` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` int unsigned NOT NULL,
  `last_activity_at` datetime DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_ip_address` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_user_agent` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `access_tokens_token_unique` (`token`),
  KEY `access_tokens_user_id_foreign` (`user_id`),
  KEY `access_tokens_type_index` (`type`),
  CONSTRAINT `access_tokens_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=226 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `access_tokens`
--

LOCK TABLES `access_tokens` WRITE;
/*!40000 ALTER TABLE `access_tokens` DISABLE KEYS */;
INSERT INTO `access_tokens` VALUES (1,'IgO67IH27IxOvvtgXvET4KHleDvf5TjEmGxSp14r',1,'2023-09-30 20:58:55','2023-09-30 20:58:55','session_remember',NULL,NULL,NULL),(3,'4r7sjWlKaN9uEH45Fh2AyDXtrMjficVfUUqJ8dhe',1,'2023-09-30 21:27:35','2023-09-30 21:27:35','session_remember',NULL,'::1','PostmanRuntime/7.32.3'),(4,'YCA69PvpLrUj8EKaRewSRPxzl16TjBPWOoO43EyG',1,'2023-09-30 21:27:57','2023-09-30 21:27:57','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(5,'j9MM3PDNjw7obly3NIbkSVLTfAPm2HNOm30vh2Z2',1,'2023-09-30 23:03:56','2023-09-30 21:28:45','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(6,'vwGHbn3F26dWqUWYLCuwFJLmZMkNqZwaPI17a3pc',1,'2023-09-30 23:29:56','2023-09-30 23:03:56','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(7,'9ShdhU1u5xH7ItBOq7BOJ4UtA7ghx0wITR2Hc3JE',1,'2023-10-01 00:44:13','2023-09-30 23:29:56','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(8,'gzmjICYTvaGsoZdOQmR7Mekv10sBfzaaKGxEXTSD',1,'2023-10-01 00:44:13','2023-10-01 00:44:13','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(9,'E3huoFZhmSPgCNGH7tPDGPn6rbkZzxqV7mNL1DSq',1,'2023-10-01 00:44:13','2023-10-01 00:44:13','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(10,'lkZvI52vOXxZN8rIuZ2uHywMb9oo9KwXG0LtQhCh',1,'2023-10-01 00:44:14','2023-10-01 00:44:14','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(11,'H8fqc4CY0ay3b8b38CqG78ZLS05yVfgJ066dP9nV',1,'2023-10-01 00:44:14','2023-10-01 00:44:14','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(12,'nYYsFOLrfdaU1I5oP2mX5FFtY1oWplWSNAqYLcDJ',1,'2023-10-01 00:44:15','2023-10-01 00:44:15','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(13,'YIJiG26abCF4toOrK4dALytUy946dlrK3SjuXnC1',1,'2023-10-01 00:44:15','2023-10-01 00:44:15','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(14,'1BaIy5fXJVkcNtLTe5NPIY9B5G6Y0YAfnzgKA03O',1,'2023-10-01 00:44:16','2023-10-01 00:44:16','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(15,'OgrLQ6e4b148yhC2zSU678XcEPyRuyhq4nQ31on1',1,'2023-10-01 00:44:16','2023-10-01 00:44:16','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(16,'oRfwAzYBA6IgJN3LaxDcrOLVdblwY3rWWEbvO7M2',1,'2023-10-01 00:44:17','2023-10-01 00:44:17','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(17,'BzBSUAcVNrUVHrSyaOcC5TuSkYel1GVXUbgYON8g',1,'2023-10-01 00:44:17','2023-10-01 00:44:17','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(18,'xLmdhFcJXwn1eLQuYgs6NanUqgd8hMt6IA8EfXhd',1,'2023-10-01 00:44:18','2023-10-01 00:44:18','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(19,'DB0HhQHL9vm5xQUgiyMIIutM8gWIFtW7PX232Ork',1,'2023-10-01 00:44:18','2023-10-01 00:44:18','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(20,'29hPtmZhMxUlt1zMUTw5wg1xneeIRTFTmTIoQiD3',1,'2023-10-01 00:44:19','2023-10-01 00:44:19','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(21,'1jWDLbLeKKRuZDK1r433fVMmagvzXn9j6VCvjXnP',1,'2023-10-01 00:44:20','2023-10-01 00:44:20','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(22,'OxiTBc4Ck6uh4IZJPH4GBIokrQWcuuLgejoWvuFE',1,'2023-10-01 00:44:20','2023-10-01 00:44:20','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(23,'LieEyFxc6PH30b1MoMuSpYNyIwVut0BLzYSzhygE',1,'2023-10-01 00:44:21','2023-10-01 00:44:21','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(24,'qus5Ym9cwSDncfp8cZBTWOjW4AREEAy3Wb5QKv5k',1,'2023-10-01 00:44:21','2023-10-01 00:44:21','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(25,'sHNjKeAEbDVgYo9lmsHeKEjwlRHttTKgicU6Lo1f',1,'2023-10-01 00:44:22','2023-10-01 00:44:22','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(26,'TJRfYZBuvncK801xKESLQvTincVP14m1JonyIlGv',1,'2023-10-01 00:44:22','2023-10-01 00:44:22','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(27,'rsyvyiAIfG1cZlgyq77zodYTI5AnEvwtX0PXwYwq',1,'2023-10-01 00:44:23','2023-10-01 00:44:23','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(28,'Hyh7OlutN77CgQU4GuvAjsA0UdQNVkOMKgRIxsBO',1,'2023-10-01 00:44:23','2023-10-01 00:44:23','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(29,'AFBhGU2LeT7NYEsQg643Z5TSrczGKhK7bf87rkYb',1,'2023-10-01 00:44:24','2023-10-01 00:44:24','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(30,'SySAlr8N7PRDzzSkukPsqtgaU8zStlLwd1zVZ1cq',1,'2023-10-01 00:44:25','2023-10-01 00:44:25','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(31,'8bjzic0VipL7OzAslWLP7AiNhLaWMKah4tcjhTyd',1,'2023-10-01 00:44:25','2023-10-01 00:44:25','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(32,'MsOzL6beALyKMuDMfktqjYt7eIPnWc07YAZ8lutL',1,'2023-10-01 00:44:26','2023-10-01 00:44:25','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(33,'dxFGxkQ93eRISqF6neomZBGbYEA4I7RZTwFmDGxj',1,'2023-10-01 00:44:26','2023-10-01 00:44:26','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(34,'32G9jSWyWDu2wZUJ7IVJGRqkRt2KPzePcIPdR1eC',1,'2023-10-01 00:44:27','2023-10-01 00:44:27','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(35,'jtKQAPIuJABYmQc8V4HyDwooJ45cTpKkbcRM2FeI',1,'2023-10-01 00:44:28','2023-10-01 00:44:28','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(36,'LinH514826Y6rRKpBpw9MxmNCprKc11dU6iWzlxL',1,'2023-10-01 00:44:28','2023-10-01 00:44:28','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(37,'EXpsMeOSFiHdKiAVKSBBNZfsgWujqAGEFL4SC9yE',1,'2023-10-01 00:44:28','2023-10-01 00:44:28','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(38,'foDJPQuCTDMUVJKA1CCxQ5DbBM7MdV4bMtYrY6fE',1,'2023-10-01 00:44:29','2023-10-01 00:44:29','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(39,'53QcAj9VyGVzefU5YrSjRfPZg66CyDyi658cCODs',1,'2023-10-01 00:44:29','2023-10-01 00:44:29','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(40,'wxMyaEJT4c5tZlCUjUINA7Fj8HZZDEuqc0zgmuhb',1,'2023-10-01 00:44:30','2023-10-01 00:44:30','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(41,'Dhp14tvqGfUZ8642K1AUZsd03OjobUukB7XA9D66',1,'2023-10-01 00:44:30','2023-10-01 00:44:30','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(42,'J0NRTc0WQwYulnNpZRNtkartTsZMsQ91TNEpjEMA',1,'2023-10-01 00:44:31','2023-10-01 00:44:31','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(43,'ZmKiMQ3S0tdqpsloTYHcmBVGD2cBWxX68XuuuBwc',1,'2023-10-01 00:44:31','2023-10-01 00:44:31','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(44,'IpysPXbtY4jiiZlffgAiizqNwaVAO2Q4g2rVaRuA',1,'2023-10-01 00:44:31','2023-10-01 00:44:31','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(45,'RFcyXd9JtPBoSM5ry7bYq6aexc3NSu8V6seawOdA',1,'2023-10-01 00:44:32','2023-10-01 00:44:32','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(46,'Uoksd1RI9T1prtxzGIMHQO0HUwkZDKGiggMPY0v7',1,'2023-10-01 00:44:32','2023-10-01 00:44:32','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(47,'016FV2hSL7sGDvZBmnJw69GyDHoPFKOrF7gDphpf',1,'2023-10-01 00:44:33','2023-10-01 00:44:33','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(48,'afmCT0NIvkYIOhr2npUf8TcbHyNAJ9RLuz3zIdUZ',1,'2023-10-01 00:44:33','2023-10-01 00:44:33','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(49,'fLByiumpiWJVqC3QWLQWnkRuqWaoSqvERcLSOcJ8',1,'2023-10-01 00:44:34','2023-10-01 00:44:34','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(50,'mO8VjPVY5xCOkmkcZdc1k48NSL0AzAnmyheJEtEO',1,'2023-10-01 00:44:34','2023-10-01 00:44:34','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(51,'ffBvflYKh73WMo49OQwAGYzxsgigITrOKmwEQKyF',1,'2023-10-01 00:44:35','2023-10-01 00:44:35','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(52,'pv3jAhJsSU5rjTHHCiwMhrtYT2falLFlfqsB3XGw',1,'2023-10-01 00:44:35','2023-10-01 00:44:35','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(53,'3Epu6gQJAx5ydcXzZVKCgQG2xknvIBQjDXqPFEAC',1,'2023-10-01 00:44:35','2023-10-01 00:44:35','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(54,'maUFZsZWXTaQiTrjUjH9Gp5P9Wi29VfcJzMRnobx',1,'2023-10-01 00:44:36','2023-10-01 00:44:36','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(55,'WJmokOWUDxOD4wQC2GV44bkEedAQCNksP4dzbexg',1,'2023-10-01 00:44:36','2023-10-01 00:44:36','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(56,'pg4hYQqFeWD5mI3JTdXgFZrjdghI4yEeW33vOCe4',1,'2023-10-01 00:44:37','2023-10-01 00:44:37','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(57,'Zft1yLHiWZGefevCpf6FLet4UGCRk3xuJjX6fG78',1,'2023-10-01 00:44:37','2023-10-01 00:44:37','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(58,'gUnqkAIZC5xmYeJI8KNciCvJAm0JKipL5RdbkyhC',1,'2023-10-01 01:02:13','2023-10-01 00:44:38','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(59,'Jc0dBfRrOwQgonzvt0tpFJWSqW6wtGwB4VFu8ITF',1,'2023-10-01 01:02:27','2023-10-01 01:02:27','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(60,'s9fhJkIzplT4hSnYAFXY5nzNMIQwxbrqnEC4zgGj',1,'2023-10-01 01:02:28','2023-10-01 01:02:28','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(61,'tE5aR2DQDtP1aciYJH9Nfdphc7kqXKNgtPB1vm0c',1,'2023-10-01 01:02:29','2023-10-01 01:02:29','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(62,'34xlKg3er8QndJWI81IpALCf9C440dOZPQXWg48p',1,'2023-10-01 01:02:30','2023-10-01 01:02:30','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(63,'3r7GJiU74VmSdWn8e6e2DGH0XV02UP3gYAaUX2Km',1,'2023-10-01 01:02:31','2023-10-01 01:02:31','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(64,'Z9JnkFmJYgRFHhzkVG9jeZCRg0ETBKJNquyeyjpu',1,'2023-10-01 01:02:31','2023-10-01 01:02:31','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(65,'DCnphvtwmk9keyZ2qexX2aTsGYbhZCt1moE4MqlP',1,'2023-10-01 01:02:32','2023-10-01 01:02:32','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(66,'tXW9CBIVpmFmULnPyb4LdHwDxZBGc7ecNyz84Ccd',1,'2023-10-01 01:02:34','2023-10-01 01:02:34','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(67,'V7yKABcDqJRHnoQvkkOb8UMmAZL1TkOUIKqtReaf',1,'2023-10-01 01:02:35','2023-10-01 01:02:35','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(68,'c9lcF6rwbHcF5P7vFsaJ73qiTnYBhmzC9Bk6MPqX',1,'2023-10-01 01:02:36','2023-10-01 01:02:36','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(69,'iMFvlimAQlBZj4NAMXpTXznH5IIMRL0Emojs0Zt6',1,'2023-10-01 01:02:37','2023-10-01 01:02:37','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(70,'jCu8XYEBsU1H4QxsJ86OhfGSS5i68ew3tsdIY9Pc',1,'2023-10-01 01:02:38','2023-10-01 01:02:38','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(71,'RyU3COqEw1NvIaOm8Enm4ntcwj8r9R5KfILnXw6b',1,'2023-10-01 01:02:39','2023-10-01 01:02:39','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(72,'QVQPvSDzFmugaAphlVQkD8gUMlqSn9iEZ49g3oRx',1,'2023-10-01 01:02:40','2023-10-01 01:02:40','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(73,'AEZl1kKRhj6CoYlcyes4XL8CPoyQPgnHhT7SMoX2',1,'2023-10-01 01:02:40','2023-10-01 01:02:40','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(74,'ePSXh06pIuzvF4Wfy3EiMXTMsQ3XqYLybgfrrBIu',1,'2023-10-01 01:02:41','2023-10-01 01:02:41','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(75,'hV3Es4ICkENHehYTcnJUsmErbcYl6aOwvLlGth5B',1,'2023-10-01 01:02:41','2023-10-01 01:02:41','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(76,'061ZDp9jqGxA8o1UezrQnCHmhCdf1lapD0musZxn',1,'2023-10-01 01:02:42','2023-10-01 01:02:42','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(77,'1X07TU38uA9dZGqaoYDlzMCtdgW0C6JCWLa8BkGH',1,'2023-10-01 01:02:42','2023-10-01 01:02:42','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(78,'AMh3T9wwMWSO02L0e5VUbxEx0Y9zQixPdysveI1N',1,'2023-10-01 01:21:55','2023-10-01 01:02:43','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(79,'wQyhzIY1Y6UHx8Kd6vNn9Sf3qJ0OPeCubRvV1w91',1,'2023-10-01 01:02:43','2023-10-01 01:02:43','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(80,'z4saYkKefOw8ZlCyJFt4332FKnh7VnJc9MZWqce8',1,'2023-10-01 01:27:06','2023-10-01 01:22:36','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(81,'xfze5VhgREg1nmJ88smJAHqDs5c6fo9MR7gF4nWf',1,'2023-10-01 01:27:07','2023-10-01 01:27:07','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(82,'1a55Kxeukh3ZW6ymY0PoIr8hIXeLRwVpfH9aegvs',1,'2023-10-01 01:28:27','2023-10-01 01:28:27','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(83,'uWZEXV9JxJBfA2f98LviIV4IjwREi9YGQMORKcxn',1,'2023-10-01 01:28:29','2023-10-01 01:28:29','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(84,'7k6qweq0ZDY94ob2rio01LV6wGb9XxhDDIRW6xbJ',1,'2023-10-01 03:08:13','2023-10-01 01:29:37','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(85,'BWl7NM9nWf9EYrMwJ8uRo0XazCGS9VDdm7oNVUDl',1,'2023-10-01 01:32:49','2023-10-01 01:29:40','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(86,'ipvdQxvIl0j6sGNAHlKpDz0yeWT1yX4JGH6PJWgl',1,'2023-10-01 01:32:49','2023-10-01 01:32:49','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(87,'rl9BZLzdJCSmafu54NZYVDFFJIm7o2hVRvfbhGzx',1,'2023-10-01 01:35:42','2023-10-01 01:33:13','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(88,'2yUVXwxuxYu6l2bWrPpRVFHDg3RnoFcxPTZ6BGzo',1,'2023-10-01 01:35:42','2023-10-01 01:35:42','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(89,'gNxms9UpnIhFsP9EpDIka1k7dcwzPoORYqIRTEcN',1,'2023-10-01 01:35:48','2023-10-01 01:35:48','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(90,'C9lYmW2u6xDXnRqpUtMaHYl9D7JOxCUHMna3neku',1,'2023-10-01 01:43:12','2023-10-01 01:35:55','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(91,'0Abg1coz0ufqhkTm1ym0LO91EpotOAVGuDiIxqcw',1,'2023-10-01 01:56:58','2023-10-01 01:43:13','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(92,'eDnCMzO6TcUbgkspxNb3Eys85PBn3DKuELbibmJQ',1,'2023-10-01 01:56:59','2023-10-01 01:56:59','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(93,'alWcvK2YRMJPAiJ822ByHBHhANhVCE27klaoPnTh',1,'2023-10-01 01:58:18','2023-10-01 01:58:18','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(94,'zYyuCEtEVvTz6p4rAyafPYCtKnfHoILLl5cdt2lV',1,'2023-10-01 01:58:35','2023-10-01 01:58:35','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(95,'EoJYFmb250rzQ6TO40oJfRgyNMWkrwJ8XjPh3N9C',1,'2023-10-01 02:19:08','2023-10-01 01:59:16','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(96,'ZifdxIvsFtbPgoRFjXrhEdsP946eRsr36leC2Idy',1,'2023-10-01 02:19:09','2023-10-01 02:19:09','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(97,'jBANb7STSBPTd9wYftf0X6qt7MsT4o9UlIXEghDW',1,'2023-10-01 02:19:55','2023-10-01 02:19:55','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(98,'4FdytdXEphzlpNLilF4VsEZZ0uJVcwSEPxupZ28Y',1,'2023-10-01 02:20:03','2023-10-01 02:20:03','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(99,'CJVPFkOdrTuJhsxXZKFe3bcWaHlsG8zTBsKBit2a',1,'2023-10-01 02:20:05','2023-10-01 02:20:05','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(100,'KUxj8FDdzprviUJMyBA6em91UghBMqMWRekdIxsW',1,'2023-10-01 02:21:15','2023-10-01 02:21:15','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(101,'zSCne2idbnqQ4VPxwDKeExc9lBE8PGNC7Bk2mDIc',1,'2023-10-01 02:22:41','2023-10-01 02:22:41','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(102,'6sZFf8jz3yCtrGDaGpGgrpZiUt1ZTA1eQLxgJZLt',1,'2023-10-01 02:26:48','2023-10-01 02:22:59','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(103,'w70Nujehy8GakleXKrsxpjveRaOcq9UKcCDVNYpi',1,'2023-10-01 02:26:49','2023-10-01 02:26:49','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(104,'hgjxj2DP7DFYT3pH34n4hnN3ukwc9jQA1Mtjk0sl',1,'2023-10-01 02:26:59','2023-10-01 02:26:59','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(105,'FjTjcPFrfs7GGSeuzLLjb8d9EvjpLa8DL5yMUiy9',1,'2023-10-01 02:36:34','2023-10-01 02:27:05','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(106,'dKsZqWDQXHpKmXfsRhwdsNy2RPgnRek9aYKAP0Mj',1,'2023-10-01 02:36:34','2023-10-01 02:36:34','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(107,'p5trEskoyIgyCjI9xvCfEcjSySYJoS7EGrMYvd92',1,'2023-10-01 02:37:06','2023-10-01 02:37:06','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(108,'KOo50zZdRTTsjjqZRmxVcXlFOYppASuyRTI78xw0',1,'2023-10-01 02:38:55','2023-10-01 02:37:09','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(109,'2l1rIOfa5SKBatw9Za3ZEDBRFrtH2BefxhpPOvUz',1,'2023-10-01 02:38:55','2023-10-01 02:38:55','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(110,'IaV5FtSWsRev5En4Gv8AZFZYHIracF6pBtB1oTq2',1,'2023-10-01 02:50:28','2023-10-01 02:39:23','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(111,'kmxutQ5BESKUnfaSQNlPP46CDaqj39ghXg7ETieF',1,'2023-10-01 02:54:05','2023-10-01 02:50:29','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(112,'GBDerq4EvXK17U5SYwGspa7MTmME2Du4MQsCZIKx',1,'2023-10-01 02:54:05','2023-10-01 02:54:05','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(113,'rcrfbigKCc7FDskUeVgFCsNPZp1O64NQ2tXmFHex',1,'2023-10-01 03:01:39','2023-10-01 02:54:56','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(114,'W7Nk3pBaPjYVz2t2ER8UwWPSrEd2jkIkZct7pDz0',1,'2023-10-01 03:03:40','2023-10-01 03:01:40','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(115,'i9x4UM2XVMOjqPn3HuviNejrZZs9AxOwUetQh2eU',1,'2023-10-01 03:03:40','2023-10-01 03:03:40','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(116,'F0iWVexCNeqeftsB0N1QoVj0qwmtk4XrrTeFa4Vf',1,'2023-10-01 03:05:09','2023-10-01 03:05:09','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(117,'FRBBbJp3Y4lkVqJ7VWMCBNkHMZbId6Jba9hZVsAT',1,'2023-10-01 03:05:24','2023-10-01 03:05:24','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(118,'lXi7wb78mQpY5NdfkfGg1X2TDr8lomhEYYh94IDT',1,'2023-10-01 03:05:38','2023-10-01 03:05:38','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(119,'KWS0txbq19K2UnsZUnsS09qQM1gkKpBnn1cfgZ8V',1,'2023-10-01 03:06:36','2023-10-01 03:06:36','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(121,'GRfW5oM2vXBgpwCGB9aJGlhqyETnP1PRVKeg0skW',1,'2023-10-01 03:08:00','2023-10-01 03:08:00','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(122,'oFSjhY8mQ3GiJEH51ZEff5SOKK6cB9XKcwx3XZGH',1,'2023-10-01 03:10:11','2023-10-01 03:08:13','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(123,'DyOAoy9PiYFzLGce2by6qH9aXdTQeiwknrAWJuWm',1,'2023-10-01 03:10:12','2023-10-01 03:10:12','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(124,'x6uSsx2U5mWSXyfyzU3giw0rZ6SuB9gnkp0eQYzm',1,'2023-10-01 03:10:30','2023-10-01 03:10:30','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(126,'kspNiZoQi64WuRoAigCRvqf47K96Lz7U4Sqxo2sW',1,'2023-10-01 03:11:54','2023-10-01 03:11:54','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(129,'IIDMwybbG9YfINNEzOtkh3Wc11hsd8mdAZ9HarhO',1,'2023-10-01 03:15:53','2023-10-01 03:15:53','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(130,'zAKaaCRa5zLh5NqZeZvBircfhQi7PI9gfFS6jFid',1,'2023-10-01 03:16:11','2023-10-01 03:16:11','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(131,'O9empiSfXPHZFpoDMTaHu5vm5XygEG0tcDzNEq7W',1,'2023-10-05 00:51:51','2023-10-01 03:17:08','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(134,'giR9QOpSNGKggOmaqP3XvD4J4KznsrsL2KGvBXXQ',1,'2023-10-01 04:16:20','2023-10-01 04:16:20','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(135,'B0HsZMHB507t6mB7Eirwq84pNwfZF47eO7Eek6VO',1,'2023-10-01 04:16:39','2023-10-01 04:16:39','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(138,'YKOsCsu82c6QxOuecoeeZDr2SRbS6PRaObaYPLYy',1,'2023-10-01 04:36:43','2023-10-01 04:27:05','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(139,'3fK4W78EVVmMhdmmxkB8sBKjhc8fBRdQhp8ktots',1,'2023-10-01 04:36:44','2023-10-01 04:36:44','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(140,'kVWtXK1ErfVxibup4JyMtabAEQhgoLdu4CXGt5OO',1,'2023-10-01 04:47:53','2023-10-01 04:37:18','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(141,'DWMSux7cZpG5F9w6rQ8mF73r3TdVJn07WFPJJBPa',1,'2023-10-01 04:47:54','2023-10-01 04:47:54','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(142,'5C8IYGXyFbmQ90SQx74bpPfGoetaH6Da8dvLTUMj',1,'2023-10-01 04:48:29','2023-10-01 04:48:29','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(143,'99HaCeAl6La0rDUyUtkLwGoPzqFn0zE7EpnU3bEq',1,'2023-10-01 04:51:11','2023-10-01 04:48:34','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(144,'YNZc1cWQ5CTwDEMW88JHEiqu7lKWtQ8jH94lbXgi',1,'2023-10-01 04:55:09','2023-10-01 04:51:11','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(145,'F5UAZKk88rybNg13pwEQrflUM9IaKgldnnuKG1Eu',1,'2023-10-01 04:55:10','2023-10-01 04:55:10','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(171,'o7wRQkQ01dR8MFpihSG2cwoU06qBp0KehjEVDM89',1,'2023-10-05 00:58:20','2023-10-05 00:51:51','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(174,'O1RIoQBn2pE0EmiflTqzGOPbsnU4RWfRFyty2XIu',1,'2023-10-05 00:58:21','2023-10-05 00:58:21','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(175,'Fg8vVJ2VLXQkhfOMO3LId0dkBEbcR3xwV13FDP01',1,'2023-10-05 01:00:08','2023-10-05 00:58:21','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(176,'NSUb0PmkncZK4JlRaWeGiUv6ob17qycrOK8mk4Kt',1,'2023-10-05 00:58:24','2023-10-05 00:58:24','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(177,'1v62jCinxMxKpTHduoTGxr47aPwG78z7gUpzPLsM',1,'2023-10-05 00:58:24','2023-10-05 00:58:24','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(178,'pD3hLbgzzdWpoUk755B7FDJUiFtGDpEC5otDY5xe',1,'2023-10-08 20:57:14','2023-10-05 01:00:09','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(189,'1DpliGXt67OrjnhQzqhM9E4JkjqGZQfDMbFTQ38z',1,'2023-10-08 19:33:34','2023-10-08 19:33:34','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(190,'hk3rRuZ5LaLDdPpgqTpVUoEqzcoNl4Q04R05Dvzl',1,'2023-10-08 19:34:07','2023-10-08 19:34:07','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(191,'WmtG9MobcXQBB9bmIArdeGYS604jAiEFmfpydgcw',1,'2023-10-08 19:40:10','2023-10-08 19:35:10','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(192,'tNpGp8c6xAha8zi51Ljh34iF8j9dwavGOYLEmdX8',1,'2023-10-08 19:41:32','2023-10-08 19:41:32','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(193,'hUkQ6MzdfceJJ8fgHgVAPcuWfBciHy5Yah0tckJL',1,'2023-10-08 19:46:04','2023-10-08 19:41:54','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(194,'y8UfDdFWS0tEq7qostN2uMhN4MIinaqtqrvCtN56',1,'2023-10-08 19:48:11','2023-10-08 19:46:33','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(195,'s3j8rrPJWql94xG2GgCzOZuJ7ZDhy6yXImPnLqw1',1,'2023-10-08 19:55:30','2023-10-08 19:49:35','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(196,'rjBibPgweU6qBzEAe3DoTkcKHM1Gdc3Ne8mMSW33',1,'2023-10-08 19:55:31','2023-10-08 19:55:31','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(197,'fmPQGhYKQ78grIOEP11wcEU3pldFe5443gyhwUdn',1,'2023-10-08 19:55:39','2023-10-08 19:55:39','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(198,'WqzOm8McASSB8qRk7alI8DAeEQEYvR8MXzg227qa',1,'2023-10-08 19:56:41','2023-10-08 19:56:41','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(199,'gJIK3xo0EGJ31at3MsYsIOs8cDarbn8lSGVZdepu',1,'2023-10-08 19:57:32','2023-10-08 19:57:32','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(200,'Bc0cv0iIJ9h51A6ZbfPJstXRTc0yFKwELdvVv8fN',1,'2023-10-08 19:57:44','2023-10-08 19:57:44','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(201,'6e3gHPWXcIcoqYgYrVWnxIV3bx0wyvO7wbq44t3Q',1,'2023-10-08 19:58:09','2023-10-08 19:58:09','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(202,'f1sMeZCtqN7qGaPyVbSKVCAGTfgHDzGvjaXPeDlu',1,'2023-10-08 19:58:12','2023-10-08 19:58:12','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(203,'Qz40PRVFlDMISsd31E6DKruVvFeeYPXLTosBQAEW',1,'2023-10-08 20:24:01','2023-10-08 19:58:14','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(204,'hugwJil29IvogajgdguBF2jrvzxNBwXLtsV45UOR',1,'2023-10-08 20:24:01','2023-10-08 20:24:01','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(205,'1yxUBHj1eo2rbZQwFX2rD994ejFHwc46j2uVnoqm',1,'2023-10-08 20:24:21','2023-10-08 20:24:21','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(206,'BFMI9kW2C5dz1gljiOUiRUqm9XR1lTT5rBxwIbpO',1,'2023-10-08 20:31:25','2023-10-08 20:24:33','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(207,'KdwFl2XcV0WQMgKqi1VQ2g2hPNFvbfOdUmY0BsPQ',1,'2023-10-29 22:27:49','2023-10-08 20:31:26','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'),(208,'R8OfIxlPbQOC8HCIQv1RJVgwHRTOh2BlzIgpk6eU',1,'2023-10-08 21:02:04','2023-10-08 21:00:26','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(210,'3S5OweRZIajrDwJoLYZXIABFJkhxiQ0Z6eP13YFN',1,'2023-10-10 19:14:38','2023-10-10 19:14:38','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'),(212,'PTGIBwqagqvVtgrjdcRpA8TzSlkDHsmnPoBE9W5Y',1,'2023-10-23 19:07:11','2023-10-23 17:59:20','session',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'),(213,'TR8Eb8toWjfG3fsFkC26KedB3SiYb1wgEnpN97ue',1,'2023-10-23 19:07:46','2023-10-23 19:07:46','session',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'),(214,'rvlzWHyswSkwzzsSXRaNzOVxiwaVHyesMtwOoO3Y',1,'2023-10-29 22:38:18','2023-10-29 22:27:50','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'),(215,'BZvUHqOSG3O1eSyMldElwKFi32L1lgW6ocBEp604',1,'2023-10-29 22:27:51','2023-10-29 22:27:51','session_remember',NULL,'::1','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'),(216,'r2xAI188sw9M653bwS87bz61rTggZQlrDr0HtxyU',4,'2024-01-09 00:51:17','2024-01-09 00:38:36','session_remember',NULL,'128.195.14.114','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'),(219,'tGkeficFWCsHiBCXCxewfNRSk145wUFxpzaKTqzb',6,'2024-01-09 00:45:27','2024-01-09 00:45:27','session_remember',NULL,'128.195.14.114','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'),(220,'FMgfrQkdpvBNPnNYmZ1raMuX8fMaxLuVlSFggorK',6,'2024-01-09 00:47:58','2024-01-09 00:47:58','session_remember',NULL,'128.195.14.114','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'),(222,'takqGwdohQk61e7SCWtnczJY0lYFVcNUnMxIFkhr',6,'2024-01-09 02:55:55','2024-01-09 02:55:55','session_remember',NULL,'128.195.14.114','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'),(225,'9NmDqdrDsw02GoB95y8zpuRXT54uUBNA8Wj1iq0u',1,'2024-01-09 03:24:05','2024-01-09 03:24:05','session_remember',NULL,'128.195.14.114','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
/*!40000 ALTER TABLE `access_tokens` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `api_keys`
--

DROP TABLE IF EXISTS `api_keys`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `api_keys` (
  `key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `allowed_ips` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `scopes` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_id` int unsigned DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `last_activity_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `api_keys_key_unique` (`key`),
  KEY `api_keys_user_id_foreign` (`user_id`),
  CONSTRAINT `api_keys_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `api_keys`
--

LOCK TABLES `api_keys` WRITE;
/*!40000 ALTER TABLE `api_keys` DISABLE KEYS */;
INSERT INTO `api_keys` VALUES ('hdebsyxiigyklxgsqivyswwiisohzlnezzzzzzzz',1,NULL,NULL,NULL,'2023-09-30 21:19:35','2023-10-01 04:14:27');
/*!40000 ALTER TABLE `api_keys` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `discussion_tag`
--

DROP TABLE IF EXISTS `discussion_tag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `discussion_tag` (
  `discussion_id` int unsigned NOT NULL,
  `tag_id` int unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`discussion_id`,`tag_id`),
  KEY `discussion_tag_tag_id_foreign` (`tag_id`),
  CONSTRAINT `discussion_tag_discussion_id_foreign` FOREIGN KEY (`discussion_id`) REFERENCES `discussions` (`id`) ON DELETE CASCADE,
  CONSTRAINT `discussion_tag_tag_id_foreign` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `discussion_tag`
--

LOCK TABLES `discussion_tag` WRITE;
/*!40000 ALTER TABLE `discussion_tag` DISABLE KEYS */;
INSERT INTO `discussion_tag` VALUES (2,1,'2023-10-09 19:31:56'),(3,1,'2023-10-09 19:46:22'),(4,2,'2023-10-23 18:24:29'),(5,1,'2024-01-09 00:41:34');
/*!40000 ALTER TABLE `discussion_tag` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `discussion_user`
--

DROP TABLE IF EXISTS `discussion_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `discussion_user` (
  `user_id` int unsigned NOT NULL,
  `discussion_id` int unsigned NOT NULL,
  `last_read_at` datetime DEFAULT NULL,
  `last_read_post_number` int unsigned DEFAULT NULL,
  `subscription` enum('follow','ignore') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`user_id`,`discussion_id`),
  KEY `discussion_user_discussion_id_foreign` (`discussion_id`),
  CONSTRAINT `discussion_user_discussion_id_foreign` FOREIGN KEY (`discussion_id`) REFERENCES `discussions` (`id`) ON DELETE CASCADE,
  CONSTRAINT `discussion_user_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `discussion_user`
--

LOCK TABLES `discussion_user` WRITE;
/*!40000 ALTER TABLE `discussion_user` DISABLE KEYS */;
INSERT INTO `discussion_user` VALUES (1,2,'2023-10-23 19:27:14',21,NULL),(1,3,'2023-10-23 19:29:46',22,NULL),(1,4,'2023-10-23 18:37:28',6,NULL),(4,5,'2024-01-09 00:41:35',1,NULL);
/*!40000 ALTER TABLE `discussion_user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `discussion_views`
--

DROP TABLE IF EXISTS `discussion_views`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `discussion_views` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned DEFAULT NULL,
  `discussion_id` int unsigned NOT NULL,
  `ip` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `visited_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `discussion_views_discussion_id_foreign` (`discussion_id`),
  KEY `discussion_views_user_id_foreign` (`user_id`),
  CONSTRAINT `discussion_views_discussion_id_foreign` FOREIGN KEY (`discussion_id`) REFERENCES `discussions` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `discussion_views_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `discussion_views`
--

LOCK TABLES `discussion_views` WRITE;
/*!40000 ALTER TABLE `discussion_views` DISABLE KEYS */;
INSERT INTO `discussion_views` VALUES (1,4,5,'128.195.14.114','2024-01-09 00:41:35'),(6,NULL,5,'128.195.14.114','2024-01-09 03:15:01');
/*!40000 ALTER TABLE `discussion_views` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `discussions`
--

DROP TABLE IF EXISTS `discussions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `discussions` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `comment_count` int NOT NULL DEFAULT '1',
  `participant_count` int unsigned NOT NULL DEFAULT '0',
  `post_number_index` int unsigned NOT NULL DEFAULT '0',
  `created_at` datetime NOT NULL,
  `user_id` int unsigned DEFAULT NULL,
  `first_post_id` int unsigned DEFAULT NULL,
  `last_posted_at` datetime DEFAULT NULL,
  `last_posted_user_id` int unsigned DEFAULT NULL,
  `last_post_id` int unsigned DEFAULT NULL,
  `last_post_number` int unsigned DEFAULT NULL,
  `hidden_at` datetime DEFAULT NULL,
  `hidden_user_id` int unsigned DEFAULT NULL,
  `slug` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `is_private` tinyint(1) NOT NULL DEFAULT '0',
  `is_approved` tinyint(1) NOT NULL DEFAULT '1',
  `is_sticky` tinyint(1) NOT NULL DEFAULT '0',
  `is_locked` tinyint(1) NOT NULL DEFAULT '0',
  `view_count` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `discussions_hidden_user_id_foreign` (`hidden_user_id`),
  KEY `discussions_first_post_id_foreign` (`first_post_id`),
  KEY `discussions_last_post_id_foreign` (`last_post_id`),
  KEY `discussions_last_posted_at_index` (`last_posted_at`),
  KEY `discussions_last_posted_user_id_index` (`last_posted_user_id`),
  KEY `discussions_created_at_index` (`created_at`),
  KEY `discussions_user_id_index` (`user_id`),
  KEY `discussions_comment_count_index` (`comment_count`),
  KEY `discussions_participant_count_index` (`participant_count`),
  KEY `discussions_hidden_at_index` (`hidden_at`),
  KEY `discussions_is_sticky_created_at_index` (`is_sticky`,`created_at`),
  KEY `discussions_is_sticky_last_posted_at_index` (`is_sticky`,`last_posted_at`),
  KEY `discussions_is_locked_index` (`is_locked`),
  FULLTEXT KEY `title` (`title`),
  CONSTRAINT `discussions_first_post_id_foreign` FOREIGN KEY (`first_post_id`) REFERENCES `posts` (`id`) ON DELETE SET NULL,
  CONSTRAINT `discussions_hidden_user_id_foreign` FOREIGN KEY (`hidden_user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `discussions_last_post_id_foreign` FOREIGN KEY (`last_post_id`) REFERENCES `posts` (`id`) ON DELETE SET NULL,
  CONSTRAINT `discussions_last_posted_user_id_foreign` FOREIGN KEY (`last_posted_user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `discussions_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `discussions`
--

LOCK TABLES `discussions` WRITE;
/*!40000 ALTER TABLE `discussions` DISABLE KEYS */;
INSERT INTO `discussions` VALUES (2,'idk',21,1,0,'2023-10-09 19:31:56',1,2,'2023-10-23 19:27:14',1,47,21,'2024-01-09 00:40:54',4,'idk',0,1,0,0,0),(3,'wdejcwbejcebwjc',22,1,0,'2023-10-09 19:46:22',1,3,'2023-10-23 19:29:45',1,50,22,'2024-01-09 00:41:00',4,'wdejcwbejcebwjc',0,1,0,0,0),(4,'hi hih i',6,1,0,'2023-10-23 18:24:29',1,33,'2023-10-23 18:37:28',1,38,6,'2024-01-09 00:41:57',4,'hi-hih-i',0,1,0,0,1),(5,'Welcome to Texera!',1,1,0,'2024-01-09 00:41:34',4,51,'2024-01-09 00:41:34',4,51,1,NULL,NULL,'welcome-to-texera',0,1,0,0,5);
/*!40000 ALTER TABLE `discussions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `email_tokens`
--

DROP TABLE IF EXISTS `email_tokens`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `email_tokens` (
  `token` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `email` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` int unsigned NOT NULL,
  `created_at` datetime DEFAULT NULL,
  PRIMARY KEY (`token`),
  KEY `email_tokens_user_id_foreign` (`user_id`),
  CONSTRAINT `email_tokens_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `email_tokens`
--

LOCK TABLES `email_tokens` WRITE;
/*!40000 ALTER TABLE `email_tokens` DISABLE KEYS */;
/*!40000 ALTER TABLE `email_tokens` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `flags`
--

DROP TABLE IF EXISTS `flags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `flags` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `post_id` int unsigned NOT NULL,
  `type` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` int unsigned DEFAULT NULL,
  `reason` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reason_detail` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `flags_post_id_foreign` (`post_id`),
  KEY `flags_user_id_foreign` (`user_id`),
  KEY `flags_created_at_index` (`created_at`),
  CONSTRAINT `flags_post_id_foreign` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `flags_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `flags`
--

LOCK TABLES `flags` WRITE;
/*!40000 ALTER TABLE `flags` DISABLE KEYS */;
/*!40000 ALTER TABLE `flags` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `group_permission`
--

DROP TABLE IF EXISTS `group_permission`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `group_permission` (
  `group_id` int unsigned NOT NULL,
  `permission` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`group_id`,`permission`),
  CONSTRAINT `group_permission_group_id_foreign` FOREIGN KEY (`group_id`) REFERENCES `groups` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `group_permission`
--

LOCK TABLES `group_permission` WRITE;
/*!40000 ALTER TABLE `group_permission` DISABLE KEYS */;
INSERT INTO `group_permission` VALUES (2,'viewForum',NULL),(3,'discussion.flagPosts','2023-09-30 20:58:55'),(3,'discussion.likePosts','2023-09-30 20:58:56'),(3,'discussion.reply',NULL),(3,'discussion.replyWithoutApproval','2023-09-30 20:58:56'),(3,'discussion.startWithoutApproval','2023-09-30 20:58:56'),(3,'searchUsers',NULL),(3,'startDiscussion',NULL),(3,'user.editOwnNickname','2024-01-09 00:40:10'),(4,'discussion.approvePosts','2023-09-30 20:58:56'),(4,'discussion.editPosts',NULL),(4,'discussion.hide',NULL),(4,'discussion.hidePosts',NULL),(4,'discussion.lock','2023-09-30 20:58:56'),(4,'discussion.rename',NULL),(4,'discussion.sticky','2023-09-30 20:58:56'),(4,'discussion.tag','2023-09-30 20:58:56'),(4,'discussion.viewFlags','2023-09-30 20:58:55'),(4,'discussion.viewIpsPosts',NULL),(4,'user.suspend','2023-09-30 20:58:56'),(4,'user.viewLastSeenAt',NULL);
/*!40000 ALTER TABLE `group_permission` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `group_user`
--

DROP TABLE IF EXISTS `group_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `group_user` (
  `user_id` int unsigned NOT NULL,
  `group_id` int unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`,`group_id`),
  KEY `group_user_group_id_foreign` (`group_id`),
  CONSTRAINT `group_user_group_id_foreign` FOREIGN KEY (`group_id`) REFERENCES `groups` (`id`) ON DELETE CASCADE,
  CONSTRAINT `group_user_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `group_user`
--

LOCK TABLES `group_user` WRITE;
/*!40000 ALTER TABLE `group_user` DISABLE KEYS */;
INSERT INTO `group_user` VALUES (1,1,'2024-01-09 03:23:43'),(4,1,'2023-09-30 20:58:55');
/*!40000 ALTER TABLE `group_user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `groups`
--

DROP TABLE IF EXISTS `groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `groups` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name_singular` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `name_plural` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `color` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `icon` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_hidden` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `groups`
--

LOCK TABLES `groups` WRITE;
/*!40000 ALTER TABLE `groups` DISABLE KEYS */;
INSERT INTO `groups` VALUES (1,'Admin','Admins','#B72A2A','fas fa-wrench',0,NULL,NULL),(2,'Guest','Guests',NULL,NULL,0,NULL,NULL),(3,'Member','Members',NULL,NULL,0,NULL,NULL),(4,'Mod','Mods','#80349E','fas fa-bolt',0,NULL,NULL);
/*!40000 ALTER TABLE `groups` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `login_providers`
--

DROP TABLE IF EXISTS `login_providers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `login_providers` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned NOT NULL,
  `provider` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `identifier` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `last_login_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `login_providers_provider_identifier_unique` (`provider`,`identifier`),
  KEY `login_providers_user_id_foreign` (`user_id`),
  CONSTRAINT `login_providers_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `login_providers`
--

LOCK TABLES `login_providers` WRITE;
/*!40000 ALTER TABLE `login_providers` DISABLE KEYS */;
/*!40000 ALTER TABLE `login_providers` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `migrations`
--

DROP TABLE IF EXISTS `migrations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `migrations` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `migration` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `extension` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=158 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `migrations`
--

LOCK TABLES `migrations` WRITE;
/*!40000 ALTER TABLE `migrations` DISABLE KEYS */;
INSERT INTO `migrations` VALUES (1,'2015_02_24_000000_create_access_tokens_table',NULL),(2,'2015_02_24_000000_create_api_keys_table',NULL),(3,'2015_02_24_000000_create_config_table',NULL),(4,'2015_02_24_000000_create_discussions_table',NULL),(5,'2015_02_24_000000_create_email_tokens_table',NULL),(6,'2015_02_24_000000_create_groups_table',NULL),(7,'2015_02_24_000000_create_notifications_table',NULL),(8,'2015_02_24_000000_create_password_tokens_table',NULL),(9,'2015_02_24_000000_create_permissions_table',NULL),(10,'2015_02_24_000000_create_posts_table',NULL),(11,'2015_02_24_000000_create_users_discussions_table',NULL),(12,'2015_02_24_000000_create_users_groups_table',NULL),(13,'2015_02_24_000000_create_users_table',NULL),(14,'2015_09_15_000000_create_auth_tokens_table',NULL),(15,'2015_09_20_224327_add_hide_to_discussions',NULL),(16,'2015_09_22_030432_rename_notification_read_time',NULL),(17,'2015_10_07_130531_rename_config_to_settings',NULL),(18,'2015_10_24_194000_add_ip_address_to_posts',NULL),(19,'2015_12_05_042721_change_access_tokens_columns',NULL),(20,'2015_12_17_194247_change_settings_value_column_to_text',NULL),(21,'2016_02_04_095452_add_slug_to_discussions',NULL),(22,'2017_04_07_114138_add_is_private_to_discussions',NULL),(23,'2017_04_07_114138_add_is_private_to_posts',NULL),(24,'2018_01_11_093900_change_access_tokens_columns',NULL),(25,'2018_01_11_094000_change_access_tokens_add_foreign_keys',NULL),(26,'2018_01_11_095000_change_api_keys_columns',NULL),(27,'2018_01_11_101800_rename_auth_tokens_to_registration_tokens',NULL),(28,'2018_01_11_102000_change_registration_tokens_rename_id_to_token',NULL),(29,'2018_01_11_102100_change_registration_tokens_created_at_to_datetime',NULL),(30,'2018_01_11_120604_change_posts_table_to_innodb',NULL),(31,'2018_01_11_155200_change_discussions_rename_columns',NULL),(32,'2018_01_11_155300_change_discussions_add_foreign_keys',NULL),(33,'2018_01_15_071700_rename_users_discussions_to_discussion_user',NULL),(34,'2018_01_15_071800_change_discussion_user_rename_columns',NULL),(35,'2018_01_15_071900_change_discussion_user_add_foreign_keys',NULL),(36,'2018_01_15_072600_change_email_tokens_rename_id_to_token',NULL),(37,'2018_01_15_072700_change_email_tokens_add_foreign_keys',NULL),(38,'2018_01_15_072800_change_email_tokens_created_at_to_datetime',NULL),(39,'2018_01_18_130400_rename_permissions_to_group_permission',NULL),(40,'2018_01_18_130500_change_group_permission_add_foreign_keys',NULL),(41,'2018_01_18_130600_rename_users_groups_to_group_user',NULL),(42,'2018_01_18_130700_change_group_user_add_foreign_keys',NULL),(43,'2018_01_18_133000_change_notifications_columns',NULL),(44,'2018_01_18_133100_change_notifications_add_foreign_keys',NULL),(45,'2018_01_18_134400_change_password_tokens_rename_id_to_token',NULL),(46,'2018_01_18_134500_change_password_tokens_add_foreign_keys',NULL),(47,'2018_01_18_134600_change_password_tokens_created_at_to_datetime',NULL),(48,'2018_01_18_135000_change_posts_rename_columns',NULL),(49,'2018_01_18_135100_change_posts_add_foreign_keys',NULL),(50,'2018_01_30_112238_add_fulltext_index_to_discussions_title',NULL),(51,'2018_01_30_220100_create_post_user_table',NULL),(52,'2018_01_30_222900_change_users_rename_columns',NULL),(55,'2018_09_15_041340_add_users_indicies',NULL),(56,'2018_09_15_041828_add_discussions_indicies',NULL),(57,'2018_09_15_043337_add_notifications_indices',NULL),(58,'2018_09_15_043621_add_posts_indices',NULL),(59,'2018_09_22_004100_change_registration_tokens_columns',NULL),(60,'2018_09_22_004200_create_login_providers_table',NULL),(61,'2018_10_08_144700_add_shim_prefix_to_group_icons',NULL),(62,'2019_10_12_195349_change_posts_add_discussion_foreign_key',NULL),(63,'2020_03_19_134512_change_discussions_default_comment_count',NULL),(64,'2020_04_21_130500_change_permission_groups_add_is_hidden',NULL),(65,'2021_03_02_040000_change_access_tokens_add_type',NULL),(66,'2021_03_02_040500_change_access_tokens_add_id',NULL),(67,'2021_03_02_041000_change_access_tokens_add_title_ip_agent',NULL),(68,'2021_04_18_040500_change_migrations_add_id_primary_key',NULL),(69,'2021_04_18_145100_change_posts_content_column_to_mediumtext',NULL),(70,'2018_07_21_000000_seed_default_groups',NULL),(71,'2018_07_21_000100_seed_default_group_permissions',NULL),(72,'2021_05_10_000000_rename_permissions',NULL),(73,'2022_05_20_000000_add_timestamps_to_groups_table',NULL),(74,'2022_05_20_000001_add_created_at_to_group_user_table',NULL),(75,'2022_05_20_000002_add_created_at_to_group_permission_table',NULL),(76,'2022_07_14_000000_add_type_index_to_posts',NULL),(77,'2022_07_14_000001_add_type_created_at_composite_index_to_posts',NULL),(78,'2022_08_06_000000_change_access_tokens_last_activity_at_to_nullable',NULL),(79,'2015_09_02_000000_add_flags_read_time_to_users_table','flarum-flags'),(80,'2015_09_02_000000_create_flags_table','flarum-flags'),(81,'2017_07_22_000000_add_default_permissions','flarum-flags'),(82,'2018_06_27_101500_change_flags_rename_time_to_created_at','flarum-flags'),(83,'2018_06_27_101600_change_flags_add_foreign_keys','flarum-flags'),(84,'2018_06_27_105100_change_users_rename_flags_read_time_to_read_flags_at','flarum-flags'),(85,'2018_09_15_043621_add_flags_indices','flarum-flags'),(86,'2019_10_22_000000_change_reason_text_col_type','flarum-flags'),(87,'2015_09_21_011527_add_is_approved_to_discussions','flarum-approval'),(88,'2015_09_21_011706_add_is_approved_to_posts','flarum-approval'),(89,'2017_07_22_000000_add_default_permissions','flarum-approval'),(90,'2015_02_24_000000_create_discussions_tags_table','flarum-tags'),(91,'2015_02_24_000000_create_tags_table','flarum-tags'),(92,'2015_02_24_000000_create_users_tags_table','flarum-tags'),(93,'2015_02_24_000000_set_default_settings','flarum-tags'),(94,'2015_10_19_061223_make_slug_unique','flarum-tags'),(95,'2017_07_22_000000_add_default_permissions','flarum-tags'),(96,'2018_06_27_085200_change_tags_columns','flarum-tags'),(97,'2018_06_27_085300_change_tags_add_foreign_keys','flarum-tags'),(98,'2018_06_27_090400_rename_users_tags_to_tag_user','flarum-tags'),(99,'2018_06_27_100100_change_tag_user_rename_read_time_to_marked_as_read_at','flarum-tags'),(100,'2018_06_27_100200_change_tag_user_add_foreign_keys','flarum-tags'),(101,'2018_06_27_103000_rename_discussions_tags_to_discussion_tag','flarum-tags'),(102,'2018_06_27_103100_add_discussion_tag_foreign_keys','flarum-tags'),(103,'2019_04_21_000000_add_icon_to_tags_table','flarum-tags'),(104,'2022_05_20_000003_add_timestamps_to_tags_table','flarum-tags'),(105,'2022_05_20_000004_add_created_at_to_discussion_tag_table','flarum-tags'),(106,'2023_03_01_000000_create_post_mentions_tag_table','flarum-tags'),(107,'2015_05_11_000000_add_suspended_until_to_users_table','flarum-suspend'),(108,'2015_09_14_000000_rename_suspended_until_column','flarum-suspend'),(109,'2017_07_22_000000_add_default_permissions','flarum-suspend'),(110,'2018_06_27_111400_change_users_rename_suspend_until_to_suspended_until','flarum-suspend'),(111,'2021_10_27_000000_add_suspend_reason_and_message','flarum-suspend'),(112,'2015_05_11_000000_add_subscription_to_users_discussions_table','flarum-subscriptions'),(113,'2015_02_24_000000_add_sticky_to_discussions','flarum-sticky'),(114,'2017_07_22_000000_add_default_permissions','flarum-sticky'),(115,'2018_09_15_043621_add_discussions_indices','flarum-sticky'),(116,'2021_01_13_000000_add_discussion_last_posted_at_indices','flarum-sticky'),(117,'2015_05_11_000000_create_mentions_posts_table','flarum-mentions'),(118,'2015_05_11_000000_create_mentions_users_table','flarum-mentions'),(119,'2018_06_27_102000_rename_mentions_posts_to_post_mentions_post','flarum-mentions'),(120,'2018_06_27_102100_rename_mentions_users_to_post_mentions_user','flarum-mentions'),(121,'2018_06_27_102200_change_post_mentions_post_rename_mentions_id_to_mentions_post_id','flarum-mentions'),(122,'2018_06_27_102300_change_post_mentions_post_add_foreign_keys','flarum-mentions'),(123,'2018_06_27_102400_change_post_mentions_user_rename_mentions_id_to_mentions_user_id','flarum-mentions'),(124,'2018_06_27_102500_change_post_mentions_user_add_foreign_keys','flarum-mentions'),(125,'2021_04_19_000000_set_default_settings','flarum-mentions'),(126,'2022_05_20_000005_add_created_at_to_post_mentions_post_table','flarum-mentions'),(127,'2022_05_20_000006_add_created_at_to_post_mentions_user_table','flarum-mentions'),(128,'2022_10_21_000000_create_post_mentions_group_table','flarum-mentions'),(129,'2021_03_25_000000_default_settings','flarum-markdown'),(130,'2015_02_24_000000_add_locked_to_discussions','flarum-lock'),(131,'2017_07_22_000000_add_default_permissions','flarum-lock'),(132,'2018_09_15_043621_add_discussions_indices','flarum-lock'),(133,'2015_05_11_000000_create_posts_likes_table','flarum-likes'),(134,'2015_09_04_000000_add_default_like_permissions','flarum-likes'),(135,'2018_06_27_100600_rename_posts_likes_to_post_likes','flarum-likes'),(136,'2018_06_27_100700_change_post_likes_add_foreign_keys','flarum-likes'),(137,'2021_05_10_094200_add_created_at_to_post_likes_table','flarum-likes'),(138,'2018_09_29_060444_replace_emoji_shorcuts_with_unicode','flarum-emoji'),(140,'2020_07_29_010101_add_post_field','kyrne-evergreen'),(141,'2019_06_07_000000_add_recipients_table','fof-byobu'),(142,'2019_06_07_000001_remove_flagrow_migrations','fof-byobu'),(143,'2019_07_08_000000_add_blocks_pd_to_users','fof-byobu'),(144,'2019_07_09_000000_blocks_pd_index','fof-byobu'),(145,'2020_02_14_214800_fix_user_id_not_nullable_for_group_pds','fof-byobu'),(146,'2020_02_19_110103_remove_retired_settings_key','fof-byobu'),(147,'2020_10_23_000000_users_unified_index_column','fof-byobu'),(148,'2021_01_13_000000_unified_index_index','fof-byobu'),(149,'2021_01_13_000001_byobu_indicies','fof-byobu'),(150,'2021_01_23_000000_drop_tags_from_old_private_discussions','fof-byobu'),(151,'2021_04_21_000000_drop_users_unified_index_column','fof-byobu'),(152,'2017_11_07_223624_discussions_add_views','michaelbelgium-discussion-views'),(153,'2018_11_30_141817_discussions_rename_views','michaelbelgium-discussion-views'),(154,'2020_01_11_220612_add_discussionviews_table','michaelbelgium-discussion-views'),(155,'2020_11_23_000000_add_nickname_column','flarum-nicknames'),(156,'2020_12_02_000001_set_default_permissions','flarum-nicknames'),(157,'2021_11_16_000000_nickname_column_nullable','flarum-nicknames');
/*!40000 ALTER TABLE `migrations` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `notifications`
--

DROP TABLE IF EXISTS `notifications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `notifications` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned NOT NULL,
  `from_user_id` int unsigned DEFAULT NULL,
  `type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `subject_id` int unsigned DEFAULT NULL,
  `data` blob,
  `created_at` datetime NOT NULL,
  `is_deleted` tinyint(1) NOT NULL DEFAULT '0',
  `read_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `notifications_from_user_id_foreign` (`from_user_id`),
  KEY `notifications_user_id_index` (`user_id`),
  CONSTRAINT `notifications_from_user_id_foreign` FOREIGN KEY (`from_user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `notifications_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `notifications`
--

LOCK TABLES `notifications` WRITE;
/*!40000 ALTER TABLE `notifications` DISABLE KEYS */;
/*!40000 ALTER TABLE `notifications` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `password_tokens`
--

DROP TABLE IF EXISTS `password_tokens`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `password_tokens` (
  `token` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` int unsigned NOT NULL,
  `created_at` datetime DEFAULT NULL,
  PRIMARY KEY (`token`),
  KEY `password_tokens_user_id_foreign` (`user_id`),
  CONSTRAINT `password_tokens_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `password_tokens`
--

LOCK TABLES `password_tokens` WRITE;
/*!40000 ALTER TABLE `password_tokens` DISABLE KEYS */;
/*!40000 ALTER TABLE `password_tokens` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `post_likes`
--

DROP TABLE IF EXISTS `post_likes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `post_likes` (
  `post_id` int unsigned NOT NULL,
  `user_id` int unsigned NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`post_id`,`user_id`),
  KEY `post_likes_user_id_foreign` (`user_id`),
  CONSTRAINT `post_likes_post_id_foreign` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `post_likes_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `post_likes`
--

LOCK TABLES `post_likes` WRITE;
/*!40000 ALTER TABLE `post_likes` DISABLE KEYS */;
/*!40000 ALTER TABLE `post_likes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `post_mentions_group`
--

DROP TABLE IF EXISTS `post_mentions_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `post_mentions_group` (
  `post_id` int unsigned NOT NULL,
  `mentions_group_id` int unsigned NOT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`post_id`,`mentions_group_id`),
  KEY `post_mentions_group_mentions_group_id_foreign` (`mentions_group_id`),
  CONSTRAINT `post_mentions_group_mentions_group_id_foreign` FOREIGN KEY (`mentions_group_id`) REFERENCES `groups` (`id`) ON DELETE CASCADE,
  CONSTRAINT `post_mentions_group_post_id_foreign` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `post_mentions_group`
--

LOCK TABLES `post_mentions_group` WRITE;
/*!40000 ALTER TABLE `post_mentions_group` DISABLE KEYS */;
/*!40000 ALTER TABLE `post_mentions_group` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `post_mentions_post`
--

DROP TABLE IF EXISTS `post_mentions_post`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `post_mentions_post` (
  `post_id` int unsigned NOT NULL,
  `mentions_post_id` int unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`post_id`,`mentions_post_id`),
  KEY `post_mentions_post_mentions_post_id_foreign` (`mentions_post_id`),
  CONSTRAINT `post_mentions_post_mentions_post_id_foreign` FOREIGN KEY (`mentions_post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `post_mentions_post_post_id_foreign` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `post_mentions_post`
--

LOCK TABLES `post_mentions_post` WRITE;
/*!40000 ALTER TABLE `post_mentions_post` DISABLE KEYS */;
/*!40000 ALTER TABLE `post_mentions_post` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `post_mentions_tag`
--

DROP TABLE IF EXISTS `post_mentions_tag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `post_mentions_tag` (
  `post_id` int unsigned NOT NULL,
  `mentions_tag_id` int unsigned NOT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`post_id`,`mentions_tag_id`),
  KEY `post_mentions_tag_mentions_tag_id_foreign` (`mentions_tag_id`),
  CONSTRAINT `post_mentions_tag_mentions_tag_id_foreign` FOREIGN KEY (`mentions_tag_id`) REFERENCES `tags` (`id`) ON DELETE CASCADE,
  CONSTRAINT `post_mentions_tag_post_id_foreign` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `post_mentions_tag`
--

LOCK TABLES `post_mentions_tag` WRITE;
/*!40000 ALTER TABLE `post_mentions_tag` DISABLE KEYS */;
/*!40000 ALTER TABLE `post_mentions_tag` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `post_mentions_user`
--

DROP TABLE IF EXISTS `post_mentions_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `post_mentions_user` (
  `post_id` int unsigned NOT NULL,
  `mentions_user_id` int unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`post_id`,`mentions_user_id`),
  KEY `post_mentions_user_mentions_user_id_foreign` (`mentions_user_id`),
  CONSTRAINT `post_mentions_user_mentions_user_id_foreign` FOREIGN KEY (`mentions_user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE,
  CONSTRAINT `post_mentions_user_post_id_foreign` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `post_mentions_user`
--

LOCK TABLES `post_mentions_user` WRITE;
/*!40000 ALTER TABLE `post_mentions_user` DISABLE KEYS */;
INSERT INTO `post_mentions_user` VALUES (5,1,'2023-10-09 19:47:13');
/*!40000 ALTER TABLE `post_mentions_user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `post_user`
--

DROP TABLE IF EXISTS `post_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `post_user` (
  `post_id` int unsigned NOT NULL,
  `user_id` int unsigned NOT NULL,
  PRIMARY KEY (`post_id`,`user_id`),
  KEY `post_user_user_id_foreign` (`user_id`),
  CONSTRAINT `post_user_post_id_foreign` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `post_user_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `post_user`
--

LOCK TABLES `post_user` WRITE;
/*!40000 ALTER TABLE `post_user` DISABLE KEYS */;
/*!40000 ALTER TABLE `post_user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `posts`
--

DROP TABLE IF EXISTS `posts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `posts` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `discussion_id` int unsigned NOT NULL,
  `number` int unsigned DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `user_id` int unsigned DEFAULT NULL,
  `type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `content` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT ' ',
  `edited_at` datetime DEFAULT NULL,
  `edited_user_id` int unsigned DEFAULT NULL,
  `hidden_at` datetime DEFAULT NULL,
  `hidden_user_id` int unsigned DEFAULT NULL,
  `ip_address` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_private` tinyint(1) NOT NULL DEFAULT '0',
  `is_approved` tinyint(1) NOT NULL DEFAULT '1',
  `reply_to` int NOT NULL,
  `reply_count` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `posts_discussion_id_number_unique` (`discussion_id`,`number`),
  KEY `posts_edited_user_id_foreign` (`edited_user_id`),
  KEY `posts_hidden_user_id_foreign` (`hidden_user_id`),
  KEY `posts_discussion_id_number_index` (`discussion_id`,`number`),
  KEY `posts_discussion_id_created_at_index` (`discussion_id`,`created_at`),
  KEY `posts_user_id_created_at_index` (`user_id`,`created_at`),
  KEY `posts_type_index` (`type`),
  KEY `posts_type_created_at_index` (`type`,`created_at`),
  FULLTEXT KEY `content` (`content`),
  CONSTRAINT `posts_discussion_id_foreign` FOREIGN KEY (`discussion_id`) REFERENCES `discussions` (`id`) ON DELETE CASCADE,
  CONSTRAINT `posts_edited_user_id_foreign` FOREIGN KEY (`edited_user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `posts_hidden_user_id_foreign` FOREIGN KEY (`hidden_user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `posts_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=52 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `posts`
--

LOCK TABLES `posts` WRITE;
/*!40000 ALTER TABLE `posts` DISABLE KEYS */;
INSERT INTO `posts` VALUES (2,2,1,'2023-10-09 19:31:56',1,'comment','<t><p>idk</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(3,3,1,'2023-10-09 19:46:22',1,'comment','<r><p><STRONG><s>**</s>sxxbnsb nxsbxsxnsx<e>**</e></STRONG></p></r>',NULL,NULL,NULL,NULL,'::1',0,1,0,1),(4,3,2,'2023-10-09 19:46:54',1,'comment','<t><p>fjyfj</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,1),(5,3,3,'2023-10-09 19:47:12',1,'comment','<r> <p><USERMENTION displayname=\"myAdmin\" id=\"1\">@myAdmin</USERMENTION></p> \n</r>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(6,3,4,'2023-10-23 17:53:08',1,'comment','<t><p>sav</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(7,3,5,'2023-10-23 17:54:51',1,'comment','<t><p>@myAdmin#6 hfu</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(8,3,6,'2023-10-23 17:55:08',1,'comment','<t><p>@myAdmin#6 sahxhb</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(9,3,7,'2023-10-23 17:55:41',1,'comment','<t><p>@myAdmin#3 23r3r</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(10,3,8,'2023-10-23 17:56:30',1,'comment','<t><p>@myAdmin#6 wgtwqtaw</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(11,3,9,'2023-10-23 17:56:36',1,'comment','<t><p>@myAdmin#7 wqfawetet</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(12,3,10,'2023-10-23 17:57:10',1,'comment','<t><p>@myAdmin#4 hawegf</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(13,3,11,'2023-10-23 17:57:21',1,'comment','<t><p>@myAdmin#12 weakjfhaewf</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(14,3,12,'2023-10-23 17:59:27',1,'comment','<t><p>@myAdmin#9 hello</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(15,2,2,'2023-10-23 18:02:19',1,'comment','<t><p>hello</p>\n</t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(16,2,3,'2023-10-23 18:02:46',1,'comment','<t><p>ewafwef</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(17,2,4,'2023-10-23 18:02:52',1,'comment','<t><p>@myAdmin#16 weagfaewgaweg</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(18,2,5,'2023-10-23 18:03:41',1,'comment','<t><p>@myAdmin#17 wefgaew</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(19,2,6,'2023-10-23 18:05:21',1,'comment','<t><p>eee</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(20,2,7,'2023-10-23 18:05:27',1,'comment','<t><p>@myAdmin#17 ewfwew</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(21,3,13,'2023-10-23 18:09:38',1,'comment','<t><p>ewagewg</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(22,3,14,'2023-10-23 18:09:47',1,'comment','<t><p>abc</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(23,3,15,'2023-10-23 18:09:52',1,'comment','<t><p>@myAdmin#11 abc</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(24,3,16,'2023-10-23 18:12:52',1,'comment','<t><p>abcd</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(25,3,17,'2023-10-23 18:13:17',1,'comment','<t><p>abcdefg</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(26,3,18,'2023-10-23 18:13:44',1,'comment','<t><p>@myAdmin#25 abcdefghijk</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,1),(27,3,19,'2023-10-23 18:18:20',1,'comment','<t><p>@myAdmin#14 123</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(28,2,8,'2023-10-23 18:20:01',1,'comment','<t><p>@myAdmin#19 eeee</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(29,2,9,'2023-10-23 18:20:12',1,'comment','<t><p>eeee</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(30,2,10,'2023-10-23 18:20:19',1,'comment','<t><p>@myAdmin#19 eeeee</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(31,2,11,'2023-10-23 18:20:37',1,'comment','<t><p>123</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(32,2,12,'2023-10-23 18:20:43',1,'comment','<t><p>@myAdmin#31 1234567</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(33,4,1,'2023-10-23 18:24:29',1,'comment','<t><p>welcome</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(34,4,2,'2023-10-23 18:24:38',1,'comment','<t><p>@myAdmin#33 good forum</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(35,4,3,'2023-10-23 18:25:24',1,'comment','<t><p>@myAdmin#33 yesi think so</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(36,4,4,'2023-10-23 18:35:56',1,'comment','<t><p>weafew</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(37,4,5,'2023-10-23 18:36:05',1,'comment','<t><p>@myAdmin#36 12345</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(38,4,6,'2023-10-23 18:37:28',1,'comment','<t><p>@myAdmin#36 1234567</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(39,2,13,'2023-10-23 18:43:42',1,'comment','<t><p>@myAdmin#16 123456789</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(40,2,14,'2023-10-23 19:01:13',1,'comment','<t><p>@myAdmin#32 1234</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(41,2,15,'2023-10-23 19:01:49',1,'comment','<t><p>@myAdmin#40 123</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(42,2,16,'2023-10-23 19:07:53',1,'comment','<t><p>@myAdmin#41 12355</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(43,2,17,'2023-10-23 19:14:37',1,'comment','<t><p>123</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(44,2,18,'2023-10-23 19:14:48',1,'comment','<t><p>@myAdmin#43 1234</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(45,2,19,'2023-10-23 19:15:28',1,'comment','<t><p>@myAdmin#43 gvuyg</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,0),(46,2,20,'2023-10-23 19:15:43',1,'comment','<t><p>@myAdmin#43 kjbk</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,0,1),(47,2,21,'2023-10-23 19:27:14',1,'comment','<t><p>@myAdmin#46 ewagfewgwe</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,46,0),(48,3,20,'2023-10-23 19:29:05',1,'comment','<t><p>@myAdmin#3 ewafeg</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,3,0),(49,3,21,'2023-10-23 19:29:16',1,'comment','<t><p>@myAdmin#26 evvevv</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,26,0),(50,3,22,'2023-10-23 19:29:45',1,'comment','<t><p>@myAdmin#4 utfiufut</p></t>',NULL,NULL,NULL,NULL,'::1',0,1,4,0),(51,5,1,'2024-01-09 00:41:34',4,'comment','<t><p>Welcome to the discussion forum of Texera!</p></t>',NULL,NULL,NULL,NULL,'128.195.14.114',0,1,0,0);
/*!40000 ALTER TABLE `posts` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `recipients`
--

DROP TABLE IF EXISTS `recipients`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `recipients` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `discussion_id` int unsigned DEFAULT NULL,
  `user_id` int unsigned DEFAULT NULL,
  `group_id` int unsigned DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `removed_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `recipients_user_id_foreign` (`user_id`),
  KEY `recipients_group_id_foreign` (`group_id`),
  KEY `recipients_removed_at_index` (`removed_at`),
  KEY `recipients_discussion_id_user_id_index` (`discussion_id`,`user_id`),
  KEY `recipients_discussion_id_group_id_index` (`discussion_id`,`group_id`),
  CONSTRAINT `recipients_discussion_id_foreign` FOREIGN KEY (`discussion_id`) REFERENCES `discussions` (`id`) ON DELETE CASCADE,
  CONSTRAINT `recipients_group_id_foreign` FOREIGN KEY (`group_id`) REFERENCES `groups` (`id`) ON DELETE CASCADE,
  CONSTRAINT `recipients_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `recipients`
--

LOCK TABLES `recipients` WRITE;
/*!40000 ALTER TABLE `recipients` DISABLE KEYS */;
/*!40000 ALTER TABLE `recipients` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `registration_tokens`
--

DROP TABLE IF EXISTS `registration_tokens`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `registration_tokens` (
  `token` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `payload` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `created_at` datetime DEFAULT NULL,
  `provider` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `identifier` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_attributes` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`token`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `registration_tokens`
--

LOCK TABLES `registration_tokens` WRITE;
/*!40000 ALTER TABLE `registration_tokens` DISABLE KEYS */;
/*!40000 ALTER TABLE `registration_tokens` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `settings`
--

DROP TABLE IF EXISTS `settings`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `settings` (
  `key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `value` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `settings`
--

LOCK TABLES `settings` WRITE;
/*!40000 ALTER TABLE `settings` DISABLE KEYS */;
INSERT INTO `settings` VALUES ('allow_hide_own_posts','reply'),('allow_post_editing','reply'),('allow_renaming','10'),('allow_sign_up','1'),('custom_less','.item-account{\ndisplay: none;\n}\n\n.Header-title a{\nfont-size: 2rem;\ncolor: black;\n}\n\n.Search-input .FormControlz{\nwidth:150%;\n}\n\nbutton.Dropdown-toggle.Button.Button--user.Button--flat {\ndisplay: none;\n}\n\n.Header-title a{\ncolor: #000000D9;\nfont-family: -apple-system, BlinkMacSystemFont, sans-serif;\nmargin: 0 0 8px;\nmargin-bottom: 0.5rem;\nfont-weight: 500;\nline-height: 1.2;\nmargin-top: 0;\n}\n\n.Header-title a {\n  font-size: 0; /* This will visually hide the text */\n  position: relative; \n}\n\n.Header-title a::after {\n  content: \"Home\"; /* This will insert the new text */\n  display: inline-block; /* Makes it behave like inline text */\n}\n\n.Header-primary{\ndisplay: hidden;\n}\n\n.Header-secondary{\nalign: left;\n}\n\n.sideNav .Dropdown--select {\n    display: none;\n}\n\n.sideNavContainer {\ndisplay: block !important;\n}\n\n.fa-reply::before {\n  display: none !important;\n}'),('default_locale','en'),('default_route','/tags'),('display_name_driver','username'),('extensions_enabled','[\"flarum-flags\",\"flarum-approval\",\"michaelbelgium-discussion-views\",\"fof-byobu\",\"flarum-sticky\",\"flarum-statistics\",\"flarum-nicknames\",\"flarum-mentions\",\"flarum-markdown\",\"flarum-lock\",\"flarum-likes\",\"flarum-lang-english\",\"flarum-emoji\"]'),('flarum-markdown.mdarea','1'),('flarum-mentions.allow_username_format','1'),('flarum-tags.max_primary_tags','10'),('flarum-tags.max_secondary_tags','3'),('flarum-tags.min_primary_tags','1'),('flarum-tags.min_secondary_tags','0'),('forum_description',''),('forum_title','Discussions'),('mail_driver','mail'),('mail_from','noreply@localhost'),('slug_driver_Flarum\\Discussion\\Discussion',''),('slug_driver_Flarum\\User\\User','default'),('theme_colored_header','0'),('theme_dark_mode','0'),('theme_primary_color','#4D698E'),('theme_secondary_color','#4D698E'),('version','1.8.5'),('welcome_message','Discuss the Texera platform & machine learning topics – this includes sharing feedback, asking questions, and more.'),('welcome_title','Welcome to Texera ');
/*!40000 ALTER TABLE `settings` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tag_user`
--

DROP TABLE IF EXISTS `tag_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tag_user` (
  `user_id` int unsigned NOT NULL,
  `tag_id` int unsigned NOT NULL,
  `marked_as_read_at` datetime DEFAULT NULL,
  `is_hidden` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`user_id`,`tag_id`),
  KEY `tag_user_tag_id_foreign` (`tag_id`),
  CONSTRAINT `tag_user_tag_id_foreign` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`) ON DELETE CASCADE,
  CONSTRAINT `tag_user_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tag_user`
--

LOCK TABLES `tag_user` WRITE;
/*!40000 ALTER TABLE `tag_user` DISABLE KEYS */;
/*!40000 ALTER TABLE `tag_user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tags`
--

DROP TABLE IF EXISTS `tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tags` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `slug` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `color` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `background_path` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `background_mode` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `position` int DEFAULT NULL,
  `parent_id` int unsigned DEFAULT NULL,
  `default_sort` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_restricted` tinyint(1) NOT NULL DEFAULT '0',
  `is_hidden` tinyint(1) NOT NULL DEFAULT '0',
  `discussion_count` int unsigned NOT NULL DEFAULT '0',
  `last_posted_at` datetime DEFAULT NULL,
  `last_posted_discussion_id` int unsigned DEFAULT NULL,
  `last_posted_user_id` int unsigned DEFAULT NULL,
  `icon` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tags_slug_unique` (`slug`),
  KEY `tags_parent_id_foreign` (`parent_id`),
  KEY `tags_last_posted_user_id_foreign` (`last_posted_user_id`),
  KEY `tags_last_posted_discussion_id_foreign` (`last_posted_discussion_id`),
  CONSTRAINT `tags_last_posted_discussion_id_foreign` FOREIGN KEY (`last_posted_discussion_id`) REFERENCES `discussions` (`id`) ON DELETE SET NULL,
  CONSTRAINT `tags_last_posted_user_id_foreign` FOREIGN KEY (`last_posted_user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL,
  CONSTRAINT `tags_parent_id_foreign` FOREIGN KEY (`parent_id`) REFERENCES `tags` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tags`
--

LOCK TABLES `tags` WRITE;
/*!40000 ALTER TABLE `tags` DISABLE KEYS */;
INSERT INTO `tags` VALUES (1,'General','general','Announcements, resources, and interesting discussions','#20BEFF',NULL,NULL,0,NULL,NULL,0,0,1,'2024-01-09 00:41:34',5,4,'fas fa-wrench',NULL,'2024-01-09 00:41:35'),(2,'Getting Started','getting-started','The first stop for new Kagglers','#FAE041',NULL,NULL,1,NULL,NULL,0,0,1,'2023-10-23 18:37:28',4,1,'fas fa-toolbox','2023-10-05 00:29:00','2023-10-23 18:37:28'),(3,'Product Feedback','product-feedback','Tell us what you love, hate, and wish for','#20BEFF',NULL,NULL,2,NULL,NULL,0,0,0,NULL,NULL,NULL,'fas fa-comment-dots','2023-10-05 00:29:16','2023-10-05 00:49:22'),(4,'Questions & Answers','questions-answers','Technical advice from other data scientists','#FAE041',NULL,NULL,3,NULL,NULL,0,0,0,NULL,NULL,NULL,'fas fa-plug','2023-10-05 00:29:46','2023-10-05 00:49:40'),(5,'Competition Hosting','competition-hosting','Advice and support on running your own competitions','#20BEFF',NULL,NULL,4,NULL,NULL,0,0,0,NULL,NULL,NULL,'fas fa-code','2023-10-05 00:30:34','2023-10-05 00:49:57'),(7,'Achievement ','achievement','Celebrate success, share achievement ','#FAE041',NULL,NULL,5,NULL,NULL,0,0,0,NULL,NULL,NULL,'fas fa-vote-yea','2023-10-05 00:35:51','2023-10-05 00:49:48');
/*!40000 ALTER TABLE `tags` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `nickname` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `is_email_confirmed` tinyint(1) NOT NULL DEFAULT '1',
  `password` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `avatar_url` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `preferences` blob,
  `joined_at` datetime DEFAULT NULL,
  `last_seen_at` datetime DEFAULT NULL,
  `marked_all_as_read_at` datetime DEFAULT NULL,
  `read_notifications_at` datetime DEFAULT NULL,
  `discussion_count` int unsigned NOT NULL DEFAULT '0',
  `comment_count` int unsigned NOT NULL DEFAULT '0',
  `read_flags_at` datetime DEFAULT NULL,
  `suspended_until` datetime DEFAULT NULL,
  `suspend_reason` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `suspend_message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `blocks_byobu_pd` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `users_username_unique` (`username`),
  UNIQUE KEY `users_email_unique` (`email`),
  KEY `users_joined_at_index` (`joined_at`),
  KEY `users_last_seen_at_index` (`last_seen_at`),
  KEY `users_discussion_count_index` (`discussion_count`),
  KEY `users_comment_count_index` (`comment_count`),
  KEY `users_blocks_byobu_pd_index` (`blocks_byobu_pd`),
  KEY `users_nickname_index` (`nickname`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` VALUES (1,'myAdmin',NULL,'xinyual3@uci.edu',1,'$2a$10$0raoWm.MtGloDa95/CDgne54rX9uoa4RlmeoCA.ce9V1axfIHECSK',NULL,NULL,'2023-09-30 20:58:55','2024-01-09 03:24:06',NULL,'2023-10-17 03:07:23',3,49,'2023-10-16 06:52:14',NULL,NULL,NULL,0),(4,'xiaozl3@uci.edu',NULL,'xiaozl3@uci.edu',1,'$2a$10$63ISJ9VtBtA7R33QqNWLxeqb9Qqg97nq6D8E4sSAfIiquLcSO9XHC',NULL,NULL,NULL,'2024-01-09 00:51:18',NULL,NULL,1,1,NULL,NULL,NULL,NULL,0),(6,'linxinyuan@gmail.com',NULL,'linxinyuan@gmail.com',1,'$2a$10$QLbj.1IBcVCfuJmoqLNm4.64sDw23rInF4NDEAw5.9D4V6bd6x0DK',NULL,NULL,NULL,'2024-01-09 02:54:23',NULL,'2024-01-09 02:55:22',0,0,NULL,NULL,NULL,NULL,0);
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-01-08 19:48:21
