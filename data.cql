CREATE KEYSPACE logs_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE logs_keyspace.logs_table (ingestion_id TEXT PRIMARY KEY,
                                       log_id TEXT,
                                       log_region TEXT,
                                       log_size INT,
                                       log_datetime TEXT,
                                       ingestion_time TIMESTAMP);
