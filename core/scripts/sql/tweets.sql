create table TABLE_NAME
(
    id                   varchar(25)                  not null primary key,
    create_at            varchar(50)                  null,
    text                 varchar(500) charset utf8mb4 null,
    in_reply_to_status   varchar(25)                  null,
    in_reply_to_user     varchar(25)                  null,
    favourites_count     int                          null,
    retweet_count        int                          null,
    lang                 varchar(10)                  null,
    is_retweet           tinyint(1)                   null,
    hashtags             varchar(500) charset utf8mb4 null,
    user_mentions        varchar(500) charset utf8mb4 null,
    user_id              varchar(25)                  null,
    user_name            varchar(500) charset utf8mb4 null,
    user_screen_name     varchar(500) charset utf8mb4 null,
    user_location        varchar(500)                 null,
    user_description     varchar(500) charset utf8mb4 null,
    user_followers_count int                          null,
    user_friends_count   int                          null,
    user_statues_count   int                          null,
    stateName            varchar(100)                 null,
    countyName           varchar(100)                 null,
    cityName             varchar(100)                 null,
    country              varchar(100)                 null,
    bounding_box         varchar(100)                 null
);

create fulltext index text on TABLE_NAME (text);

