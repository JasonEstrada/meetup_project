-- ============================================================
-- MEETUP - DDL RAW LAYER
-- ============================================================

-- Create the database
CREATE DATABASE IF NOT EXISTS MEETUP;

-- Create the schema
USE DATABASE MEETUP;
CREATE SCHEMA IF NOT EXISTS RAW;

-- Create the tables
CREATE OR REPLACE TABLE MEETUP.RAW.DIM_CATEGORIES (
    category_id     NUMBER(2,0)     NOT NULL,
    category_name   VARCHAR,
    shortname       VARCHAR,
    sort_name       VARCHAR,
    CONSTRAINT pk_dim_categories PRIMARY KEY (category_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.DIM_CITIES (
    city_id                  NUMBER(5,0)     NOT NULL,
    city                     VARCHAR,
    country                  VARCHAR,
    distance                 NUMBER(7,3),
    latitude                 NUMBER(10,8),
    localized_country_name   VARCHAR,
    longitude                NUMBER(11,8),
    member_count             NUMBER(6,0),
    ranking                  NUMBER(3,0),
    state                    VARCHAR,
    zip                      VARCHAR,
    CONSTRAINT pk_dim_cities PRIMARY KEY (city_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.DIM_GROUPS (
    group_id                     NUMBER(8,0)     NOT NULL,
    category_id                  NUMBER(2,0),
    category_name                VARCHAR,
    category_shortname           VARCHAR,
    city_id                      NUMBER(5,0),
    city                         VARCHAR,
    country                      VARCHAR,
    created                      TIMESTAMP_NTZ,
    description                  VARCHAR,
    group_photo_base_url         VARCHAR,
    group_photo_highres_link     VARCHAR,
    group_photo_photo_id         NUMBER(9,0),
    group_photo_photo_link       VARCHAR,
    group_photo_thumb_link       VARCHAR,
    group_photo_type             VARCHAR,
    join_mode                    VARCHAR,
    lat                          NUMBER(10,8),
    link                         VARCHAR,
    lon                          NUMBER(11,8),
    members                      NUMBER(5,0),
    group_name                   VARCHAR,
    organizer_member_id          NUMBER(9,0),
    organizer_name               VARCHAR,
    organizer_photo_base_url     VARCHAR,
    organizer_photo_highres_link VARCHAR,
    organizer_photo_photo_id     NUMBER(9,0),
    organizer_photo_photo_link   VARCHAR,
    organizer_photo_thumb_link   VARCHAR,
    organizer_photo_type         VARCHAR,
    rating                       NUMBER(3,2),
    state                        VARCHAR,
    timezone                     VARCHAR,
    urlname                      VARCHAR,
    utc_offset                   NUMBER(5,0),
    visibility                   VARCHAR,
    who                          VARCHAR,
    CONSTRAINT pk_dim_groups PRIMARY KEY (group_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.DIM_MEMBERS (
    member_id       NUMBER(9,0)     NOT NULL,
    group_id        NUMBER(8,0)     NOT NULL,
    bio             VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    hometown        VARCHAR,
    joined          TIMESTAMP_NTZ,
    lat             NUMBER(10,8),
    link            VARCHAR,
    lon             NUMBER(11,8),
    member_name     VARCHAR,
    state           VARCHAR,
    member_status   VARCHAR,
    visited         TIMESTAMP_NTZ,
    CONSTRAINT pk_dim_members PRIMARY KEY (member_id, group_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.DIM_TOPICS (
    topic_id        NUMBER(7,0)     NOT NULL,
    description     VARCHAR,
    link            VARCHAR,
    members         NUMBER(8,0),
    topic_name      VARCHAR,
    urlkey          VARCHAR,
    main_topic_id   NUMBER(5,0),
    CONSTRAINT pk_dim_topics PRIMARY KEY (topic_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.DIM_VENUES (
    venue_id                 NUMBER(8,0)     NOT NULL,
    venue_name               VARCHAR,
    address_1                VARCHAR,
    city                     VARCHAR,
    country                  VARCHAR,
    distance                 NUMBER(5,2),
    lat                      NUMBER(10,8),
    localized_country_name   VARCHAR,
    lon                      NUMBER(11,8),
    rating                   NUMBER(3,2),
    rating_count             NUMBER(6,0),
    state                    VARCHAR,
    zip                      VARCHAR,
    normalised_rating        NUMBER(3,2),
    CONSTRAINT pk_dim_venues PRIMARY KEY (venue_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.FACT_EVENTS (
    event_id                    VARCHAR         NOT NULL,
    created                     TIMESTAMP_NTZ,
    description                 VARCHAR,
    duration                    NUMBER(7,0),
    event_url                   VARCHAR,
    fee_accepts                 VARCHAR,
    fee_amount                  NUMBER(8,2),
    fee_currency                VARCHAR,
    fee_description             VARCHAR,
    fee_label                   VARCHAR,
    fee_required                BOOLEAN,
    group_created               TIMESTAMP_NTZ,
    group_lat                   NUMBER(10,8),
    group_lon                   NUMBER(11,8),
    group_id                    NUMBER(8,0),
    group_join_mode             VARCHAR,
    group_name                  VARCHAR,
    group_urlname               VARCHAR,
    group_who                   VARCHAR,
    headcount                   NUMBER(5,0),
    how_to_find_us              VARCHAR,
    maybe_rsvp_count            NUMBER(5,0),
    event_name                  VARCHAR,
    photo_url                   VARCHAR,
    rating_average              NUMBER(4,2),
    rating_count                NUMBER(5,0),
    rsvp_limit                  NUMBER(5,0),
    event_status                VARCHAR,
    event_time                  TIMESTAMP_NTZ,
    updated                     TIMESTAMP_NTZ,
    utc_offset                  NUMBER(7,0),
    venue_address_1             VARCHAR,
    venue_address_2             VARCHAR,
    venue_city                  VARCHAR,
    venue_country               VARCHAR,
    venue_id                    NUMBER(8,0),
    venue_lat                   NUMBER(10,8),
    venue_localized_country     VARCHAR,
    venue_lon                   NUMBER(11,8),
    venue_name                  VARCHAR,
    venue_phone                 VARCHAR,
    venue_repinned              BOOLEAN,
    venue_state                 VARCHAR,
    venue_zip                   VARCHAR,
    visibility                  VARCHAR,
    waitlist_count              NUMBER(5,0),
    why                         VARCHAR,
    yes_rsvp_count              NUMBER(5,0),
    CONSTRAINT pk_fact_events PRIMARY KEY (event_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.BRIDGE_GROUPS_TOPICS (
    group_id    NUMBER(8,0)     NOT NULL,
    topic_id    NUMBER(7,0)     NOT NULL,
    topic_key   VARCHAR,
    topic_name  VARCHAR,
    CONSTRAINT pk_bridge_groups_topics PRIMARY KEY (group_id, topic_id)
);

CREATE OR REPLACE TABLE MEETUP.RAW.BRIDGE_MEMBERS_TOPICS (
    member_id   NUMBER(9,0)     NOT NULL,
    topic_id    NUMBER(7,0)     NOT NULL,
    topic_key   VARCHAR,
    topic_name  VARCHAR,
    CONSTRAINT pk_bridge_members_topics PRIMARY KEY (member_id, topic_id)
);