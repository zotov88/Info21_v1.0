drop type if exists status_en cascade;
drop table if exists peers cascade;
drop table if exists tasks cascade;
drop table if exists checks cascade;
drop table if exists p2p cascade;
drop table if exists verter cascade;
drop table if exists transferred_points cascade;
drop table if exists friends cascade;
drop table if exists recommendation cascade;
drop table if exists xp cascade;
drop table if exists time_tracking cascade;
drop procedure if exists export() cascade;
drop procedure if exists import() cascade;

create type status_en as enum ('Start', 'Success', 'Failure');

create table if not exists peers
(
    nickname text not null primary key,
    birthday date not null
);

create table if not exists tasks
(
    title       text not null primary key,
    parent_task text,
    max_xp      int  not null
);

create table if not exists checks
(
    id   bigint not null primary key,
    peer text   not null,
    task text   not null,
    date date   not null default current_date,
    constraint fk_checks_peers foreign key (peer) references peers (nickname),
    constraint fk_checks_tasks foreign key (task) references tasks (title)
);

create table if not exists p2p
(
    id            bigint    not null primary key,
    check_        bigint    not null,
    checking_peer text      not null,
    state         status_en not null,
    time          time      not null default current_time,
    constraint fk_p2p_check foreign key (check_) references checks (id),
    constraint fk_p2p_checking_peer foreign key (checking_peer) references peers (nickname)
);

create table if not exists verter
(
    id     bigint    not null primary key,
    check_ bigint    not null,
    state  status_en not null,
    time   time      not null default current_time,
    constraint fk_verter_checks foreign key (check_) references checks (id)
);

create table if not exists transferred_points
(
    id            bigint not null primary key,
    checking_peer text   not null,
    checked_peer  text   not null,
    points_amount int    not null,
    constraint fk_transferred_points_checking_peer foreign key (checking_peer) references peers (nickname),
    constraint fk_transferred_points_checked_peer foreign key (checked_peer) references peers (nickname)
);

create table if not exists friends
(
    id    bigint not null primary key,
    peer1 text   not null,
    peer2 text   not null,
    constraint fk_friends_peer1 foreign key (peer1) references peers (nickname),
    constraint fk_friends_peer2 foreign key (peer2) references peers (nickname)
);

create table if not exists recommendation
(
    id               bigint not null primary key,
    peer             text   not null,
    recommended_peer text   not null,
    constraint fk_recommendation_peer foreign key (peer) references peers (nickname),
    constraint fk_recommendation_recommended_peer foreign key (recommended_peer) references peers (nickname),
    constraint uq_recommendation UNIQUE (peer, recommended_peer)
);

create table if not exists xp
(
    id        bigint  not null primary key,
    check_    bigint  not null,
    xp_amount integer not null,
    constraint fk_xp_check foreign key (check_) references checks (id)
);

create table if not exists time_tracking
(
    id    bigint   not null primary key,
    peer  text     not null,
    date  date     not null default current_date,
    time  time     not null default current_time,
    state smallint not null,
    constraint fk_time_tracing_peer foreign key (peer) references peers (nickname),
    constraint ck_state check (state = 1 or state = 2)
);

create or replace procedure export_with_no_headers(in tablename varchar, in path text, in separator char) as
$$
begin
    execute format('COPY %s TO ''%s'' delimiter ''%s'' CSV;', tablename, path, separator);
end;
$$ language plpgsql;

create or replace procedure export_with_headers(in tablename varchar, in path text, in separator char) as
$$
begin
    execute format('COPY %s TO ''%s'' delimiter ''%s'' CSV HEADER;', tablename, path, separator);
end;
$$ language plpgsql;

create or replace procedure import(in tablename varchar, in path text, in separator char) as
$$
begin
    execute format('COPY %s from ''%s'' delimiter ''%s'' CSV;', tablename, path, separator);
end;
$$ language plpgsql;

-- ******************

CALL import('peers', '/tmp/peers.csv', ',');
CALL import('tasks', '/tmp/tasks.csv', ',');
CALL import('checks', '/tmp/checks.csv', ',');
CALL import('p2p', '/tmp/p2p.csv', ',');
CALL import('verter', '/tmp/verter.csv', ',');
CALL import('transferred_points', '/tmp/transferred_points.csv', ',');
CALL import('friends', '/tmp/friends.csv', ',');
CALL import('recommendation', '/tmp/recommendations.csv', ',');
CALL import('xp', '/tmp/xp.csv', ',');
CALL import('time_tracking', '/tmp/timetracking.csv', ',');

-- CALL export_with_no_headers('peers', '/tmp/peers.csv', ',');
-- CALL export_with_no_headers('tasks', '/tmp/tasks.csv', ',');
-- CALL export_with_no_headers('checks', '/tmp/checks.csv', ',');
-- CALL export_with_no_headers('p2p', '/tmp/p2p.csv', ',');
-- CALL export_with_no_headers('verter', '/tmp/verter.csv', ',');
-- CALL export_with_no_headers('transferred_points', '/tmp/transferred_points.csv', ',');
-- CALL export_with_no_headers('friends', '/tmp/friends.csv', ',');
-- CALL export_with_no_headers('recommendation', '/tmp/recommendations.csv', ',');
-- CALL export_with_no_headers('xp', '/tmp/xp.csv', ',');
-- CALL export_with_no_headers('time_tracking', '/tmp/timetracking.csv', ',');
