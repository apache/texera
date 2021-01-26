CREATE TABLE TABLE_NAME
(
    id                   BIGINT                       NOT NULL PRIMARY KEY,
    create_at            DATETIME                     NULL,
    text                 VARCHAR(500) charset utf8mb4 NULL,
    in_reply_to_status   VARCHAR(25)                  NULL,
    in_reply_to_user     VARCHAR(25)                  NULL,
    favourites_count     INT                          NULL,
    retweet_count        INT                          NULL,
    lang                 VARCHAR(10)                  NULL,
    is_retweet           TINYINT(1)                   NULL,
    hashtags             VARCHAR(500) charset utf8mb4 NULL,
    user_mentions        VARCHAR(500) charset utf8mb4 NULL,
    user_id              VARCHAR(25)                  NULL,
    user_name            VARCHAR(500) charset utf8mb4 NULL,
    user_screen_name     VARCHAR(500) charset utf8mb4 NULL,
    user_location        VARCHAR(500)                 NULL,
    user_description     VARCHAR(500) charset utf8mb4 NULL,
    user_followers_count INT                          NULL,
    user_friends_count   INT                          NULL,
    user_statues_count   INT                          NULL,
    stateName            VARCHAR(100)                 NULL,
    countyName           VARCHAR(100)                 NULL,
    cityName             VARCHAR(100)                 NULL,
    country              VARCHAR(100)                 NULL,
    bounding_box         VARCHAR(100)                 NULL
);

CREATE FULLTEXT INDEX text_index ON TABLE_NAME (text);
CREATE INDEX create_at_index ON TABLE_NAME (create_at);

