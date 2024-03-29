CREATE TABLE QUERIES (
	ID INTEGER IDENTITY PRIMARY KEY,
	DB VARCHAR(100) NOT NULL,
	QUERY TEXT
);


CREATE TABLE USERDB (
	SVR VARCHAR(100),
	TS TIMESTAMP, 
 	USR VARCHAR(100),
 	DB	VARCHAR(100),
 	LOG_CNT INT,
 	LOG_DUR FLOAT,
 	CON_CNT INT,
 	MST_IP	VARCHAR(16),
 	MST_IP_CNT INT,
 	MST_APP VARCHAR(100),
 	MST_QUERY INT REFERENCES QUERIES(ID),
 	DEBUG_CNT INT, LOG_CNT INT, INFO_CNT INT, NOTICE_CNT INT, WARNING_CNT INT, ERROR_CNT INT, FATAL_CNT INT, PANIC_CNT INT, UNKNOWN_CNT INT,
	BEGIN_CNT INT, SET_CNT INT, COMMIT_CNT INT, PARSE_CNT INT, DISCARD_ALL_CNT INT, SELECT_CNT INT, UPDATE_CNT INT, INSERT_CNT INT, IDLE_CNT INT, OTHER_CNT INT,
	BEGIN_DUR INT, SET_DUR INT, COMMIT_DUR INT, PARSE_DUR INT, DISCARD_ALL_DUR INT, SELECT_DUR INT, UPDATE_DUR INT, INSERT_DUR INT, IDLE_DUR INT, OTHER_DUR INT
);