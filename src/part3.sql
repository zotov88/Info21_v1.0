-- **************************** task 1 ****************************
create or replace function get_transferred_points()
    returns table
            (
                "Peer1"        text,
                "Peer2"        text,
                "PointsAmount" int
            )
as
$$
begin
    return query
        select tp.checking_peer,
               tp.checked_peer,
               coalesce(tp.points_amount - tp2.points_amount, tp.points_amount)
        from transferred_points tp
                 left join transferred_points tp2
                           on tp2.checking_peer = tp.checked_peer
                               and tp2.checked_peer = tp.checking_peer;
end;
$$ language plpgsql;

-- select * from get_transferred_points();

-- **************************** task 2 ****************************
create or replace function get_success_checks()
    returns table
            (
                check_id bigint
            )
as
$$
begin
    return query
        select c.id
        from checks c
                 join p2p p on p.check_ = c.id
        where p.state = 'Success';
end;
$$ language plpgsql;

create or replace function get_table_recieved_xp()
    returns table
            (
                "Peer" text,
                "Task" text,
                "XP"   int
            )
as
$$
begin
    return query
        select c.peer,
               c.task,
               coalesce(xp.xp_amount, 0)
        from checks c
                 join get_success_checks() sc on sc.check_id = c.id
                 join xp on xp.check_ = c.id;
end;
$$ language plpgsql;

-- select * from get_table_recieved_xp();

-- **************************** task 3 ****************************
create or replace function get_peers_whom_not_leave_campus(in day date)
    returns table
            (
                "Peer" text
            )
as
$$
begin
    return query
        select peer
        from time_tracking t
        where date = day
          and state = 1
        except all
        select peer
        from time_tracking t1
        where date = day
          and state = 2;
end;
$$ language plpgsql;

-- select * from get_peers_whom_not_leave_campus('2020-12-20');

-- **************************** task 4 ****************************
create or replace function get_points_change_transferred_points()
    returns table
            (
                "Peer"         text,
                "PointsChange" int
            )
as
$$
begin
    return query
        select tp.checking_peer,
               coalesce(tp.points_amount - tp2.points_amount, tp.points_amount)
        from transferred_points tp
                 left join transferred_points tp2
                           on tp2.checking_peer = tp.checked_peer
                               and tp2.checked_peer = tp.checking_peer;
end;
$$ language plpgsql;

-- select * from get_points_change_transferred_points();

-- **************************** task 5 ****************************
create or replace function get_points_change()
    returns table
            (
                "Peer"         text,
                "PointsChange" int
            )
as
$$
begin
    return query
        select "Peer1",
               "PointsAmount"
        from get_transferred_points();
end;
$$ language plpgsql;

-- select * from get_points_change();

-- **************************** task 6 ****************************
create or replace function get_frequent_task()
    returns table
            (
                "Day"  date,
                "Task" text
            )
as
$$
begin
    return query
        with counts as
                 (select date,
                         task,
                         (select count(task)
                          from checks ch
                          where ch.task = c.task
                            and ch.date = c.date) as amount
                  from checks c)
        select distinct date, task
        from counts c
        where amount = (select max(amount)
                        from counts co
                        where co.date = c.date)
        order by date desc;
end;
$$ language plpgsql;

-- select * from get_frequent_task();

-- **************************** task 7 ****************************
create or replace function get_peers_by_block(block_name text)
    returns table
            (
                nickname text,
                day      date
            )
as
$$
begin
    return query
        select Peers.nickname   as Peer,
               MAX(Checks.date) as Day
        from Peers
                 join Checks on Peers.nickname = Checks.peer
                 join tasks on Checks.task = Tasks.title
        where EXISTS (select 1
                      from Checks
                      where Peers.nickname = Checks.peer
                        and Checks.task similar to concat(block_name, '%')
                      group by Checks.peer
                      having count(distinct Checks.task) = (select count(distinct title)
                                                            from tasks
                                                            where title similar to concat(block_name, '%')))
        group by Peers.nickname
        order by MAX(checks.date) desc;
end;
$$ language plpgsql;

-- select * from get_peers_by_block('A');
-- select * from get_peers_by_block('DO');

-- **************************** task 8 ****************************
create or replace function get_best_peer()
    returns table
            (
                "Peer"            text,
                "RecommendedPeer" text
            )
as
$$
begin
    return query
        select peer,
               recommended_peer
        from (select peer,
                     recommended_peer,
                     (select count(recommended_peer)
                      from recommendation rc
                      where rc.recommended_peer = r.recommended_peer) as amount
              from recommendation r
              order by peer) res;
end;
$$ language plpgsql;

-- select * from get_best_peer();

-- **************************** task 9 ****************************
create or replace function get_percent_peer_start_blocks(in block1 text,
                                                         in block2 text)
    returns table
            (
                "StartedBlock1"      bigint,
                "StartedBlock2"      bigint,
                "StartedBothBlocks"  bigint,
                "DidntStartAnyBlock" bigint
            )
as
$$
begin
    return query
        with amount as
                 (select (select count(*)
                          from (select distinct peer
                                from checks ch
                                where ch.task like block1 || '%') res) as start1,
                         (select count(*)
                          from (select distinct peer
                                from checks ch
                                where ch.task like block2 || '%') res) as start2,
                         (select count(*)
                          from (select distinct peer
                                from checks ch
                                where ch.task like block1 || '%'
                                intersect
                                select distinct peer
                                from checks ch
                                where ch.task like block2 || '%') res) as start1_and_start2,
                         (select count(*)
                          from (select nickname
                                from peers
                                except
                                select distinct peer
                                from checks ch
                                where ch.task like block1 || '%'
                                   or ch.task like block2 || '%') res) as start_nothing)
        select (select 100 / (count(*) / start1) from peers),
               (select 100 / (count(*) / start2) from peers),
               (select 100 / (count(*) / start1_and_start2) from peers),
               (select 100 / (count(*) / start_nothing) from peers)
        from amount;
end;
$$ language plpgsql;

-- select * from get_percent_peer_start_blocks('DO', 'CPP');
-- select * from get_percent_peer_start_blocks('A', 'DO');

-- **************************** task 10 ****************************
create or replace function get_unsuccess_checks()
    returns table
            (
                check_id bigint
            )
as
$$
begin
    return query
        select c.id
        from checks c
                 join p2p p on p.check_ = c.id
        where p.state = 'Failure';
end;
$$ language plpgsql;

create or replace function get_percent_peer_complete_check_birthday()
    returns table
            (
                "SuccessfulChecks"   bigint,
                "UnsuccessfulChecks" bigint
            )
as
$$
begin
    return query
        with amount as
                 (select (select count(*)
                          from (select distinct peer
                                from checks c
                                         join get_success_checks() sc on sc.check_id = c.id
                                         join peers p on p.nickname = c.peer
                                where extract(day from c.date) = extract(day from p.birthday)
                                  and extract(month from c.date) = extract(month from p.birthday)) res) as success,
                         (select count(*)
                          from (select distinct peer
                                from checks c
                                         join get_unsuccess_checks() sc on sc.check_id = c.id
                                         join peers p on p.nickname = c.peer
                                where extract(day from c.date) = extract(day from p.birthday)
                                  and extract(month from c.date) = extract(month from p.birthday)) res) as unsuccess)
        select (select 100 / (count(*) / success) from peers),
               (select 100 / (count(*) / unsuccess) from peers)
        from amount;
end;
$$ language plpgsql;

-- select * from get_percent_peer_complete_check_birthday();

-- **************************** task 11 ****************************
create or replace function get_peers_whom_complete_and_uncomplete(in task1 text,
                                                                  in task2 text,
                                                                  in task3 text)
    returns table
            (
                "Peer" text
            )
as
$$
begin
    return query
        select peer
        from (select peer
              from checks c
                       join get_success_checks() sc on sc.check_id = c.id
              where task = task1
              intersect
              select peer
              from checks c
                       join get_success_checks() sc on sc.check_id = c.id
              where task = task2
              intersect
              select peer
              from checks c
                       join get_success_checks() sc on sc.check_id = c.id
              where task <> task3) res;
end;
$$ language plpgsql;

-- select * from get_peers_whom_complete_and_uncomplete('A1_Maze', 'DO3_Linux_Monitoring', 'A2_Simple_Navigator');
-- select * from get_peers_whom_complete_and_uncomplete('A1_Maze', 'DO1_Linux', 'C_s21_String+');

-- **************************** task 12 ****************************
create or replace function get_count_previous_tasks_for_current()
    returns table
            (
                "Task"      text,
                "PrevCount" int
            )
as
$$
begin
    return query
        with recursive amount as
                           (select t1.title,
                                   0 as counter,
                                   t1.parent_task
                            from tasks t1
                            where t1.parent_task is null
                            union
                            select t2.title,
                                   a.counter + 1,
                                   t2.parent_task
                            from tasks t2
                                     join amount a on a.title = t2.parent_task)
        select title,
               counter
        from amount;
end;
$$ language plpgsql;

-- select * from get_count_previous_tasks_for_current();

-- **************************** task 13 ****************************
create or replace function lucky_day(in N int)
    returns table
            (
                date date
            )
as
$$
begin
    return query
        with t as
                 (select *
                  from checks
                           join p2p on checks.id = p2p.check_
                           left join verter on checks.id = verter.check_
                           join tasks on checks.task = tasks.title
                           join xp on checks.id = xp.check_
                  where p2p.state = 'Success'
                    and (verter.state = 'Success' or verter.state is null))
        select t.date
        from t
        where t.xp_amount >= t.max_xp * 0.8
        group by t.date
        having count(t.date) >= N;
end;
$$ language plpgsql;

select *
from lucky_day(2);

-- **************************** task 14 ****************************
create or replace function get_peer_with_more_xp()
    returns table
            (
                "Peer" text,
                "XP"   bigint
            )
as
$$
begin
    return query
        select distinct peer,
                        (select sum(xp_amount)
                         from xp
                                  join checks c on xp.check_ = c.id
                                  join get_success_checks() sc on sc.check_id = c.id
                         where c.peer = p.nickname) amount
        from checks c
                 join get_success_checks() sc on sc.check_id = c.id
                 join peers p on p.nickname = c.peer
        order by amount desc
        limit 1;
end;
$$ language plpgsql;

-- select * from get_peer_with_more_xp();

-- **************************** task 15 ****************************
create or replace function get_peer_came_before(in arrival_time time,
                                                in N int)
    returns table
            (
                "Peer" text
            )
as
$$
begin
    return query
        select peer
        from (select nickname as peer,
                     (select count(*) as amount
                      from time_tracking tt
                      where tt.peer = p.nickname
                        and state = 1
                        and time < arrival_time)
              from peers p) peers_
        where amount >= N;
end;
$$ language plpgsql;

-- select * from get_peer_came_before('10:00', 1);
-- select * from get_peer_came_before('10:00', 2);
-- select * from get_peer_came_before('10:00', 0);
-- select * from get_peer_came_before('11:00', 1);
-- select * from get_peer_came_before('11:00', 2);
-- select * from get_peer_came_before('11:00', 3);

-- **************************** task 16 ****************************
create or replace function get_peer_left_campus(in N date,
                                                in M int)
    returns table
            (
                "Peer" text
            )
as
$$
begin
    return query
        select peer
        from (select nickname as peer,
                     (select count(*) as amount
                      from time_tracking tt
                      where tt.peer = p.nickname
                        and state = 1
                        and date > N)
              from peers p) peers_
        where amount >= M;
end;
$$ language plpgsql;

-- select * from get_peer_left_campus('2019-01-01', 0);
-- select * from get_peer_left_campus('2022-01-01', 1);
-- select * from get_peer_left_campus('2022-01-01', 2);
-- select * from get_peer_left_campus('2022-01-01', 3);
-- select * from get_peer_left_campus('2022-01-01', 4);

-- **************************** task 17 ****************************
create or replace function get_early_entries()
    returns table
            (
                "Month"        text,
                "EarlyEntries" bigint
            )
as
$$
begin
    return query -- 17th
        with amount_entries as
                 (select to_char(gs.d::timestamp, 'Month')                             as month_,
                         (select count(*)
                          from (select t.date
                                from time_tracking t
                                         join peers p on p.nickname = t.peer and state = 1
                                where extract(month from t.date) = extract(month from p.birthday)) a
                          where extract(month from a.date) = extract(month from gs.d)) as amount_common,
                         (select count(*)
                          from (select t.date
                                from time_tracking t
                                         join peers p on p.nickname = t.peer and state = 1
                                where t.time < '12:00'
                                  and extract(month from t.date) = extract(month from p.birthday)) a
                          where extract(month from a.date) = extract(month from gs.d)) as amount_early
                  from generate_series('2000-01-01', '2000-12-01', '1 month'::interval) gs(d))
        select month_,
               case
                   when amount_common = 0 or amount_early = 0
                       then 0
                   else (100 * (amount_early / amount_common::real))::bigint
                   end
        from amount_entries;
end;
$$ language plpgsql;

-- select * from get_early_entries();