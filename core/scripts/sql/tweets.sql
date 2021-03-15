CREATE TABLE TABLE_NAME
(
    id                    BIGINT                       NOT NULL PRIMARY KEY,
    created_at            DATETIME                     NULL,
    text                  VARCHAR(500) CHARSET utf8mb4 NULL,
    in_reply_to_status_id BIGINT                       NULL,
    in_reply_to_user_id   BIGINT                       NULL,
    favourites_count      INT                          NULL,
    retweet_count         INT                          NULL,
    lang                  VARCHAR(10)                  NULL,
    retweeted             BOOLEAN                      NULL,
    hashtags              VARCHAR(500) CHARSET utf8mb4 NULL,
    user_mentions         VARCHAR(500) CHARSET utf8mb4 NULL,
    user_id               BIGINT                       NULL,
    user_name             VARCHAR(500) CHARSET utf8mb4 NULL,
    user_screen_name      VARCHAR(500) CHARSET utf8mb4 NULL,
    user_location         VARCHAR(500)                 NULL,
    user_description      VARCHAR(500) CHARSET utf8mb4 NULL,
    user_followers_count  INT                          NULL,
    user_friends_count    INT                          NULL,
    user_statues_count    INT                          NULL,
    stateName             VARCHAR(100)                 NULL,
    countyName            VARCHAR(100)                 NULL,
    cityName              VARCHAR(100)                 NULL,
    country               VARCHAR(100)                 NULL,
    bounding_box          VARCHAR(500)                 NULL
);

CREATE FULLTEXT INDEX text_index on TABLE_NAME (text);
CREATE INDEX created_at_index ON TABLE_NAME (created_at);
