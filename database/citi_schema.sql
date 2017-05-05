DROP DATABASE IF EXISTS
  `citibike`;
SET
  default_storage_engine = InnoDB;
CREATE DATABASE IF NOT EXISTS citibike DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
USE
  citibike;

-- Tables 
CREATE TABLE bike_usage(
  `bu_id` INT(16) UNSIGNED NOT NULL AUTO_INCREMENT,
  result_id INT(16) UNSIGNED NOT NULL,
  bike_id INT(250) NOT NULL,
  acc_distance FLOAT NOT NULL,
  acc_time FLOAT NOT NULL,
  end_station_id INT(16) NOT NULL,
  velocity FLOAT,
  PRIMARY KEY(bu_id)
);

CREATE TABLE broken_bikes(
  `broken_id` INT(16) UNSIGNED NOT NULL AUTO_INCREMENT,
  bike_id INT(16) NOT NULL,
  last_station_id INT(16) NOT NULL,
  last_time DATETIME NOT NULL,
  zero_sum INT(16) NOT NULL,
  PRIMARY KEY(broken_id)
);

-- Tables 
CREATE TABLE heatmap(
  `heat_id` INT(16) UNSIGNED NOT NULL AUTO_INCREMENT,
  station_id VARCHAR(250) NOT NULL,
  time_slot DATETIME NOT NULL,
  in_out INT NOT NULL,
  count INT NOT NULL,
  PRIMARY KEY(heat_id),
  FOREIGN KEY (station_id) REFERENCES stations(station_id)
);

CREATE TABLE stations(
  `station_id` INT(16),
  la FLOAT NOT NULL,
  lo FLOAT NOT NULL,
  address VARCHAR(250) NOT NULL,
  PRIMARY KEY(station_id)
);