# Auxiliary mysql database

DROP DATABASE emojis;
CREATE DATABASE emojis DEFAULT CHARSET utf8mb4;
USE emojis;

CREATE TABLE emoji (
	emo_id		INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	emo_emoji	TINYBLOB,
	emo_name	VARCHAR(512),
	INDEX idx_emo_emoji (emo_emoji(16))
)ENGINE=INNODB;

CREATE TABLE project (
	pro_id		INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	pro_name	VARCHAR(32)
)ENGINE=INNODB;

CREATE TABLE emoji_project (
	emp_id		INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	emp_emoid	INT UNSIGNED,
	emp_proid	INT UNSIGNED,
	emp_count	BIGINT DEFAULT 0,
	FOREIGN KEY fk_emp_emoid (emp_emoid) REFERENCES emoji (emo_id),
	FOREIGN KEY fk_emp_proid (emp_proid) REFERENCES project (pro_id)
)ENGINE=INNODB;

CREATE TABLE follow (
	fol_id		INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	fol_emo_start	INT UNSIGNED,
	fol_emo_end	INT UNSIGNED,
	fol_count	BIGINT DEFAULT 0,
	fol_created_at	VARCHAR(24),
	fol_index	VARCHAR(16),
	fol_tweet_id	BIGINT,
	fol_hour	TINYINT,
	fol_day	TINYINT,
	FOREIGN KEY fk_fol_emo_start (fol_emo_start) REFERENCES emoji (emo_id),
	FOREIGN KEY fk_fol_emo_end (fol_emo_end) REFERENCES emoji (emo_id),
	INDEX idx_all (fol_emo_start,fol_emo_end,fol_created_at,fol_index,fol_tweet_id),
	INDEX idx_fol_index (fol_index),
	INDEX idx_fol_hour (fol_hour),
	INDEX idx_fol_day (fol_day)
)ENGINE=INNODB;

# After loading all the data into the FOLLOW table, run the following SQL:
# UPDATE follow SET fol_hour=HOUR(fol_created_at) WHERE fol_hour IS NULL;
# UPDATE follow SET fol_day=DAY(fol_created_at) WHERE fol_day IS NULL;

# In the following table, the key_date_start and key_date_end fields define the time range where keyphrases appear
# However, these fields can be NULL, and in that case, we are considering all keyphrases
CREATE TABLE keyphrase (
	key_id 	INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	key_emo_start 	INT UNSIGNED,
	key_emo_end 	INT UNSIGNED,
	key_index 	VARCHAR(16),
	key_id24 	TINYINT,
	key_date_start DATETIME,
	key_date_end 	DATETIME,
	key_count 	INT UNSIGNED,
	FOREIGN KEY fk_key_emo_start (key_emo_start) REFERENCES emoji (emo_id),
	FOREIGN KEY fk_key_emo_end (key_emo_end) REFERENCES emoji (emo_id),
	INDEX idx_key_index (key_index),
	INDEX idx_key_id24 (key_id24)
)ENGINE=INNODB;

CREATE TABLE phrase (
	phr_id 	BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	phr_keyid 	INT UNSIGNED,
	phr_keyphrase 	VARCHAR(280),
	phr_weight 	DOUBLE,
	phr_count 	INT,
	phr_idf 	DOUBLE,
	phr_tfidf 	DOUBLE COMMENT 'count * idf',
	phr_weightidf 	DOUBLE COMMENT 'weight * idf',
	FOREIGN KEY fk_phr_keyid (phr_keyid) REFERENCES keyphrase (key_id) ON DELETE CASCADE,
	INDEX idx_phr_keyphrase (phr_keyphrase)
)ENGINE=INNODB DEFAULT CHARSET utf8mb4;

-- Auxiliary table to place partial results before consolidating them and sending them to the phrase table
CREATE TABLE auxiliar (
	aux_id 	BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	aux_keyid 	INT UNSIGNED,
	aux_keyphrase 	VARCHAR(280),
	aux_weight 	DOUBLE,
	FOREIGN KEY fk_aux_keyid (aux_keyid) REFERENCES keyphrase (key_id) ON DELETE CASCADE,
	INDEX idx_aux_keyphrase (aux_keyphrase),
	INDEX idx_aux_keyid_keyphrase (aux_keyid,aux_keyphrase)
)ENGINE=INNODB DEFAULT CHARSET utf8mb4;



