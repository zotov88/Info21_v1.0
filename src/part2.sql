create or replace function check_on_start_p2p(in peer_ text,
                                              in checking_peer_ text,
                                              in task_ text) returns bigint as
$$
begin
    return id from (select ch.id,
                           ch.task,
                           ch.peer,
                           checking_peer
                    from checks ch
                             join P2P p on p.check_ = ch.id and p.state = 'Start'
                    except all
                    select ch.id, ch.task, ch.peer, checking_peer
                    from checks ch
                             join P2P p on p.check_ = ch.id and p.state <> 'Start') pe
        where pe.task = task_ and pe.peer = peer_ and pe.checking_peer = checking_peer_;
end;
$$ language plpgsql;


-- *** процедура добавления P2P проверки ***
create or replace function add_check(in peer_ text,
                                     in checking_peer_ text,
                                     in task_ text,
                                     in state_ Status_en,
                                     in time_ time) returns void as
$$
begin
    -- проверить что нет такой записи (start and status = start) при попытке добавить новую а старая не закончена
    if ((select check_on_start_p2p is null
         from check_on_start_p2p(peer_,
                                 checking_peer_,
                                 task_))
        and state_ <> 'Start')
    then
        return;
    end if;
    if ((select check_on_start_p2p is not null
         from check_on_start_p2p(peer_,
                                 checking_peer_,
                                 task_))
        and state_ = 'Start')
    then
        return;
    end if;
    if (state_ = 'Start') then
        insert into checks
        values ((select coalesce(max(id) + 1, 1) from checks),
                peer_,
                task_,
                current_date);
        insert into P2P
        values ((select coalesce(max(id) + 1, 1) from P2P),
                (select max(id) from checks),
                checking_peer_,
                state_,
                time_);
    else
        -- проверяет начата ли проверка статус которой хотим изменить, если нет заканчивает отработку
        if ((select count(*) = 0
             from check_on_start_p2p(peer_,
                                     checking_peer_,
                                     task_)))
        then
            return;
        end if;
        insert into P2P
        values ((select coalesce(max(id) + 1, 1) from P2P),
                (select *
                 from check_on_start_p2p(peer_,
                                         checking_peer_,
                                         task_)),
                checking_peer_,
                state_,
                time_);
    end if;
end;
$$ language plpgsql;

-- select *
-- from add_check('jambo', 'baky', 'C4_s21_matrix', 'Start', '13:56');
-- select *
-- from add_check('jambo', 'baky', 'C4_s21_matrix', 'Success', '13:56');
-- select *
-- from add_check('jambo', 'baky', 'C4_s21_matrix', 'Failure', '13:56');
-- select *
-- from add_check('blountam', 'jambo', 'C4_s21_matrix', 'Start', '10:51');
-- select *
-- from add_check('blountam', 'jambo', 'C4_s21_matrix', 'Success', '10:51');

create or replace function get_success_p2p(in peer_ text, in task_ text)
    returns bigint as
$$
begin
    return c.id
        from checks c
            join p2p p on p.check_ = c.id
        where p.state = 'Success' and c.peer = peer_ and c.task = task_ order by time desc limit 1;
end;
$$ language plpgsql;

-- ***
create or replace function check_on_start_verter(in peer_ text,
                                                 in task_ text)
    returns bigint as
$$
begin
    return c.* from (select cs.*
                     from get_success_p2p(peer_,
                                          task_) cs
                              join verter v on v.check_ = cs.* and v.state = 'Start'
                     except all
                     select cs.*
                     from get_success_p2p(peer_,
                                          task_) cs
                              join verter v on v.check_ = cs.* and v.state <> 'Start') c;
end;
$$ language plpgsql;

-- *** процедура добавления проверки Verter'ом ***
create or replace function add_verter_check(in peer_ text,
                                            in task_ text,
                                            in state_ Status_en,
                                            in time_ time)
    returns void as
$$
begin
    if ((select check_on_start_verter is null
         from check_on_start_verter(peer_,
                                    task_))
        and state_ <> 'Start')
    then
        return;
    end if;
    if ((select check_on_start_verter is not null
         from check_on_start_verter(peer_,
                                    task_))
        and state_ = 'Start')
    then
        return;
    end if;
    -- если нет успешной проверки
    if ((select count(*) = 0
         from (select c.id,
                      peer,
                      task
               from checks c
                        join p2p p on p.check_ = c.id
               where p.state = 'Success'
               order by time desc) c
         where c.peer = peer_
           and c.task = task_
         limit 1))
    then
        return;
    end if;
    insert into verter
    values ((select coalesce(max(id) + 1, 1) from verter),
            (select c.id
             from (select c.id, peer, task
                   from checks c
                            join p2p p on p.check_ = c.id
                   where p.state = 'Success'
                   order by time desc) c
             where c.peer = peer_
               and c.task = task_
             limit 1),
            state_,
            time_);
end;
$$ language plpgsql;

select *
from add_verter_check('blountam', 'A1_Maze', 'Start', '09:09');
select *
from add_verter_check('blountam', 'A1_Maze', 'Success', '09:44');

select *
from add_check('blountam', 'reido', 'A1_Maze', 'Success', '09:44');

-- select *
-- from add_verter_check('jambo', 'C4_s21_matrix', 'Start', '18:50');
-- select *
-- from add_verter_check('jambo', 'C4_s21_matrix', 'Success', '18:50');
-- select *
-- from add_verter_check('jambo', 'C4_s21_matrix', 'Failure', '18:50');
-- select *
-- from add_verter_check('batu khan', 'DO1_Linux', 'Start', '10:51');
-- select *
-- from add_verter_check('batu khan', 'DO1_Linux', 'Success', '10:51');


-- *** триггер: после добавления записи со статутом "start" в таблицу P2P,
-- изменить соответствующую запись в таблице transferred_points ***

create or replace function fnc_trg_p2p_insert_audit()
    returns trigger as
$$
begin
    if (new.state = 'Start') then
        if ((select count(*) > 0
             from transferred_points t
             where t.checking_peer = new.checking_peer
               and t.checked_peer = (select peer
                                     from checks c
                                     where new.check_ = c.id)))
        then
            update transferred_points t
            set points_amount = points_amount + 1
            where t.checking_peer = new.checking_peer
              and t.checked_peer = (select peer
                                    from checks c
                                    where new.check_ = c.id);
        else
            insert into transferred_points
            values ((select coalesce(max(id) + 1, 1)
                     from transferred_points),
                    new.checking_peer,
                    (select peer from checks c where new.check_ = c.id),
                    1);
        end if;
    end if;
    return new;
end;
$$ language plpgsql;

create or replace trigger trg_p2p_insert_audit
    after insert
    on p2p
    for each row
execute procedure fnc_trg_p2p_insert_audit();

select *
from add_check('baky', 'jambo', 'C4_s21_matrix', 'Start', '13:56');
select *
from add_check('baky', 'jambo', 'C4_s21_matrix', 'Success', '13:56');
-- select *
-- from add_check('jambo', 'baky', 'C4_s21_matrix', 'Start', '13:56');
-- select *
-- from add_check('jambo', 'baky', 'C4_s21_matrix', 'Success', '13:56');
-- select *
-- from add_check('blountam', 'reido', 'A1_Maze', 'Start', '09:09');
-- select *
-- from add_check('blountam', 'reido', 'A1_Maze', 'Success', '09:44');


-- *** триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи ***
create or replace function fnc_trg_xp_insert_audit()
    returns trigger as
$$
begin
    if ((select max(id) is null
         from checks))
    then
        return null;
    end if;
    if ((select new.check_ > max(c.id)
         from checks c))
    then
        return null;
    end if;
    if ((select new.xp_amount > max_xp
         from (select c.task
               from checks c
               where c.id = new.check_) curr
                  join tasks t on t.title = curr.task))
    then
        return null;
    end if;
    if ((select p.state <> 'Success'
         from checks c
                  join p2p p on p.check_ = c.id
         where c.id = new.check_
           and p.state <> 'Start'))
    then
        return null;
    end if;
    if (select count(*) = 1
        from checks c
                 join p2p p on p.check_ = c.id
        where c.id = new.check_)
    then
        return null;
    end if;
    return new;
end;
$$ language plpgsql;


create or replace trigger trg_xp_insert_audit
    before insert
    on xp
    for each row
execute procedure fnc_trg_xp_insert_audit();


-- select *
-- from add_check('jambo', 'blountam', 'C5_s21_Calc', 'Start', '11:11');
-- select *
-- from add_check('jambo', 'blountam', 'C5_s21_Calc', 'Success', '12:12');
-- -- -- -- --
-- insert into xp values ((select coalesce(max(id)+1, 1) from xp), 35, 490);
-- insert into xp values ((select coalesce(max(id)+1, 1) from xp), 1, 251);
-- insert into xp values ((select coalesce(max(id)+1, 1) from xp), 1, 245);
-- insert into xp values ((select max(id)+1 from xp), 2, 800);
-- insert into xp values ((select max(id)+1 from xp), 2, 300);
-- insert into xp values ((select max(id)+1 from xp), 60, 300);