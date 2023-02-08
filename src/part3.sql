-- 3.1
CREATE
    OR REPLACE FUNCTION fnc_transferredpoints()
    RETURNS TABLE
            (
                Peer1        VARCHAR,
                Peer2        VARCHAR,
                PointsAmount INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY WITH tmp AS (SELECT t1.id           AS id_1,
                                     t2.id           AS id_2,
                                     t1.CheckingPeer AS Checking_1,
                                     t1.CheckedPeer  AS Checked_1,
                                     t1.PointsAmount AS Points_1,
                                     t2.CheckingPeer AS Checking_2,
                                     t2.CheckedPeer  AS Checked_2,
                                     t2.PointsAmount AS Points_2
                              FROM TransferredPoints t1
                                       LEFT OUTER JOIN TransferredPoints t2 ON t1.checkedpeer = t2.checkingpeer
                                  AND t1.checkingpeer = t2.checkedpeer)
                 SELECT tmp.Checking_1,
                        tmp.Checked_1,
                        (COALESCE(Points_1, 0) - COALESCE(Points_2, 0))
                 FROM tmp
                 WHERE (id_1 < id_2)
                    OR (id_2 IS NULL);
END;
$$
    LANGUAGE 'plpgsql';

SELECT *
FROM fnc_transferredpoints();

-- 3.2
CREATE
    OR REPLACE FUNCTION fnc_checks()
    RETURNS TABLE
            (
                Peer VARCHAR,
                Task VARCHAR,
                XP   INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY WITH tmp_checks AS (SELECT checks.id, Checks.Peer AS peer, Checks.Task AS checks_task
                                     FROM Checks
                                              JOIN P2P p ON p."Check" = Checks.ID AND p.State = 'Success'
                                     EXCEPT ALL
                                     SELECT checks.id, Checks.Peer AS peer, Checks.Task AS checks_task
                                     FROM Checks
                                              JOIN Verter V on Checks.ID = V."Check" AND v.state = 'Failure'),
                      tmp_tasks_xp AS (SELECT DISTINCT Checks.id, xp.xpamount AS XP, Checks.task AS tasks_title
                                       FROM Checks
                                                JOIN xp ON xp."Check" = Checks.id)
                 SELECT tmp_checks.peer, checks_task, tmp_tasks_xp.XP
                 FROM tmp_checks
                          JOIN tmp_tasks_xp ON tmp_checks.id = tmp_tasks_xp.id
                 ORDER BY peer;
END;
$$
    LANGUAGE 'plpgsql';

SELECT *
FROM fnc_checks();

-- 3.3
CREATE
    OR REPLACE FUNCTION fnc_timetracking(IN pdate DATE)
    RETURNS TABLE
            (
                Peer VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT timetracking.peer
                  FROM timetracking
                  WHERE state = 1
                    AND "Date" = pdate
                  GROUP BY timetracking.peer
                  HAVING COUNT(state) = 1)
                 EXCEPT ALL
                 ((SELECT timetracking.peer
                   FROM timetracking
                   WHERE state = 1
                     AND "Date" = pdate)
                  EXCEPT ALL
                  (SELECT timetracking.peer
                   FROM timetracking
                   WHERE state = 2
                     AND "Date" = pdate));
END;
$$
    LANGUAGE 'plpgsql';

SELECT *
FROM fnc_timetracking('2022-10-11');

-- 3.4
CREATE OR REPLACE PROCEDURE pr_percent_of_checks(
    INOUT SuccessfulChecks NUMERIC,
    INOUT UnsuccessfulChecks NUMERIC
) AS
$$
DECLARE
    amount INTEGER;
BEGIN
    SELECT COUNT(id) INTO amount FROM checks;
    SELECT (SELECT (ROUND(100 * COUNT(*)::NUMERIC / amount, 2))
            FROM (SELECT DISTINCT checks.id
                  FROM checks
                           JOIN P2P p ON p."Check" = Checks.ID AND p.State = 'Success'
                           JOIN Verter ON Checks.ID = Verter."Check" AND Verter.State = 'Success') AS tmp) AS SuccessfulChecks,
           (SELECT(ROUND(100 * COUNT(*)::NUMERIC / amount, 2))
            FROM (SELECT DISTINCT checks.id
                  FROM checks
                           LEFT OUTER JOIN P2P p ON p."Check" = Checks.ID
                           LEFT OUTER JOIN Verter ON Checks.ID = Verter."Check"
                  WHERE p.state = 'Failure'
                     OR verter.state = 'Failure') AS tmp)                                                  AS UnsuccessfulChecks
    INTO SuccessfulChecks,UnsuccessfulChecks;
END;
$$ LANGUAGE plpgsql;

CALL pr_percent_of_checks(0, 0);

-- 3.5
CREATE OR REPLACE FUNCTION fnc_get_transferredpoints(
    IN ch_peer_nickname_in VARCHAR
)
    RETURNS INTEGER AS
$$
DECLARE
    POINTS INTEGER;
BEGIN
    WITH tmp AS (SELECT t1.checkingpeer, sum(t1.pointsamount) AS num1
                 FROM transferredpoints t1
                 GROUP BY t1.checkingpeer),
         tmp2 AS (SELECT t2.checkedpeer, sum(t2.pointsamount) AS num2
                  FROM transferredpoints t2
                  GROUP BY t2.checkedpeer),
         tmp3 AS (SELECT COALESCE(tmp.checkingpeer, tmp2.checkedpeer) AS Peer,
                         (COALESCE(num1, 0) - COALESCE(num2, 0))      AS PointsChange
                  from tmp
                           FULL OUTER JOIN tmp2 ON tmp2.checkedpeer = tmp.checkingpeer)
    SELECT PointsChange
    INTO POINTS
    FROM tmp3
    WHERE tmp3.Peer = ch_peer_nickname_in;
    RETURN POINTS;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_count_transferredpoints(
    result_data INOUT REFCURSOR
)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN result_data FOR
        SELECT Nickname                                         AS Peer,
               COALESCE(fnc_get_transferredpoints(Nickname), 0) AS PointsChange
        FROM Peers
        ORDER BY PointsChange DESC;
END;
$$;

BEGIN;
CALL pr_count_transferredpoints('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.6
CREATE OR REPLACE FUNCTION fnc_get_transferredpoints_from_function(
    IN ch_peer_nickname_in VARCHAR
)
    RETURNS INTEGER AS
$$
DECLARE
    POINTS INTEGER;
BEGIN
    WITH tmp AS (SELECT t1.Peer1, sum(t1.pointsamount) AS num1
                 FROM fnc_transferredpoints() t1
                 GROUP BY t1.Peer1),
         tmp2 AS (SELECT t2.Peer2, sum(t2.pointsamount) AS num2
                  FROM fnc_transferredpoints() t2
                  GROUP BY t2.Peer2),
         tmp3 AS (SELECT COALESCE(tmp.Peer1, tmp2.Peer2) AS Peer,
                         (COALESCE(num1, 0) - COALESCE(num2, 0))      AS PointsChange
                  from tmp
                           FULL OUTER JOIN tmp2 ON tmp2.Peer2 = tmp.Peer1)
    SELECT PointsChange
    INTO POINTS
    FROM tmp3
    WHERE tmp3.Peer = ch_peer_nickname_in;
    RETURN POINTS;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_count_transferredpoints(
    result_data INOUT REFCURSOR
)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN result_data FOR
        SELECT Nickname                                                       AS Peer,
               COALESCE(fnc_get_transferredpoints_from_function(Nickname), 0) AS PointsChange
        FROM Peers
        ORDER BY PointsChange DESC;
END;
$$;

BEGIN;
CALL pr_count_transferredpoints('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.7
CREATE OR REPLACE FUNCTION fnc_get_most_checked(
    IN ch_checks_date_in DATE
)
    RETURNS TABLE
            (
                task VARCHAR
            )
AS
$$
DECLARE
    COUNTER INTEGER;
BEGIN
    SELECT COUNT(checks.task) AS T
    INTO COUNTER
    FROM checks
    WHERE "Date" = ch_checks_date_in
    GROUP BY checks.task
    ORDER BY T DESC
    LIMIT 1;

    RETURN QUERY
        SELECT checks.task
        FROM checks
        WHERE "Date" = ch_checks_date_in
        GROUP BY checks.task
        HAVING COUNT(checks.task) = COUNTER;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_get_most_checked(
    result_data INOUT REFCURSOR
)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN result_data FOR
        SELECT DISTINCT "Date"                       AS Day,
                        fnc_get_most_checked("Date") AS Task
        FROM checks
        ORDER BY "Date" DESC;
END;
$$;

BEGIN;
CALL pr_get_most_checked('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.8
CREATE OR REPLACE PROCEDURE pr_get_last_check_time(
    INOUT Duration TIME
) AS
$$
BEGIN
    WITH tmp AS (SELECT DISTINCT checks.id, p1."Time" AS p1_time, p2."Time" AS p2_time, checks."Date"
                 FROM checks
                          JOIN p2p p1 ON p1."Check" = checks.id
                          JOIN p2p p2 ON p2."Check" = p1."Check"
                 WHERE p1.state = 'Start'
                   AND p2.state IN ('Success', 'Failure')
                 GROUP BY checks.id, p1."Time", p2."Time", checks."Date"),
         tmp2 AS (SELECT tmp.p1_time AS t1, tmp.p2_time AS t2
                  FROM tmp
                  WHERE "Date" IN (SELECT MAX("Date") FROM tmp))
    SELECT (t2 - t1)
    INTO Duration
    FROM tmp2
    WHERE tmp2.t2 IN (SELECT MAX(tmp2.t2) FROM tmp2);
END;
$$ LANGUAGE plpgsql;

CALL pr_get_last_check_time(NULL);

-- 3.9
CREATE OR REPLACE FUNCTION fnc_block_done(IN block_in varchar)
    RETURNS TABLE
            (
                Peer VARCHAR,
                Day  DATE
            )
AS
$$
BEGIN
    RETURN QUERY (WITH tmp AS (SELECT title FROM tasks WHERE title ~ ('' || block_in || '')),
                       tmp2 AS (SELECT checks.peer, checks.task, checks."Date", xp.xpamount
                                FROM checks
                                         JOIN xp ON checks.id = xp."Check"
                                WHERE checks.task ~ ('' || block_in || '')),
                       tmp3 AS (SELECT nickname, tmp.title, xpamount
                                FROM peers
                                         CROSS JOIN tmp
                                         LEFT OUTER JOIN tmp2 ON tmp2.task = tmp.title AND peers.nickname = tmp2.peer),
                       tmp4 AS (SELECT nickname
                                FROM tmp3
                                EXCEPT
                                SELECT nickname
                                FROM tmp3
                                WHERE xpamount IS NULL)
                  SELECT nickname, MAX("Date") AS Day
                  FROM tmp4
                           JOIN tmp2 ON tmp2.peer = nickname
                  GROUP BY nickname
                  ORDER BY Day DESC);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_block_done(result_data INOUT REFCURSOR, IN block_in varchar) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT *
        FROM fnc_block_done(block_in);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_block_done('data', 'SQL');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.10
CREATE OR REPLACE FUNCTION fnc_get_recommendation_peer(ch_peers_nickname_in VARCHAR)
    RETURNS VARCHAR AS
$$
DECLARE
    answer VARCHAR;
BEGIN
    WITH tmp AS (SELECT nickname, recommendedpeer, COUNT(recommendedpeer) AS C
                 FROM peers
                          LEFT OUTER JOIN friends ON peers.nickname = friends.peer2
                          LEFT OUTER JOIN recommendations ON friends.peer1 = recommendations.peer
                 GROUP BY nickname, recommendedpeer),
         tmp2 AS (SELECT *, rank() OVER (PARTITION BY nickname ORDER BY C DESC) AS r
                  FROM tmp)
    SELECT recommendedpeer
    INTO answer
    FROM tmp2
    WHERE r = 1
      AND nickname = ch_peers_nickname_in AND ch_peers_nickname_in <> recommendedpeer;
    RETURN answer;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_get_recommendation_peer(result_data INOUT REFCURSOR)
AS
$$
BEGIN
    OPEN result_data FOR
        SELECT Nickname, fnc_get_recommendation_peer(Nickname) AS recomended_peer
        FROM Peers;
END;
$$ language plpgsql;

BEGIN;
CALL pr_get_recommendation_peer('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.11
CREATE OR REPLACE FUNCTION fnc_percent_of_block(IN block_f VARCHAR, IN block_s VARCHAR)
    RETURNS TABLE
            (
                StartedBlock1      NUMERIC,
                StartedBlock2      NUMERIC,
                StartedBothBlocks  NUMERIC,
                DidntStartAnyBlock NUMERIC
            )
AS
$$
DECLARE
    amount INTEGER;
BEGIN
    SELECT COUNT(peers.nickname) INTO amount FROM peers;
    RETURN QUERY (WITH tmp AS (SELECT peers.nickname, checks.task
                               FROM peers
                                        LEFT OUTER JOIN checks ON peers.nickname = checks.peer),
                       tmp2 AS (SELECT DISTINCT tmp.nickname FROM tmp WHERE task ~ ('' || block_f || '')),
                       tmp3 AS (SELECT DISTINCT tmp.nickname FROM tmp WHERE task ~ ('' || block_s || ''))
                  SELECT ROUND(((SELECT COUNT(nickname) FROM tmp2)::NUMERIC / amount * 100), 2),
                         ROUND(((SELECT COUNT(nickname) FROM tmp3)::NUMERIC / amount * 100), 2),
                         ROUND(((SELECT COUNT(tmp2.nickname)
                                 FROM tmp2
                                          JOIN tmp3 ON tmp2.nickname = tmp3.nickname)::NUMERIC / amount * 100), 2),
                         ROUND(((SELECT COUNT(nickname)
                                 FROM (SELECT nickname
                                       FROM tmp
                                       EXCEPT
                                       ((SELECT nickname FROM tmp2) UNION (SELECT nickname FROM tmp3))) AS tmp5)::NUMERIC /
                                amount * 100), 2));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_percent_of_block(result_data INOUT REFCURSOR, IN block_f VARCHAR, IN block_s VARCHAR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT *
        FROM fnc_percent_of_block(block_f, block_s);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_percent_of_block('data', 'CPP', 'SQL');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.12
CREATE OR REPLACE PROCEDURE pr_get_friends_num(
    N INTEGER, result_data INOUT REFCURSOR
) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT Peer1 AS Peer, COUNT(Peer2) AS FriendsCount
        FROM friends
        GROUP BY peer1
        ORDER BY FriendsCount DESC
        LIMIT N;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_get_friends_num(3, 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.13
CREATE OR REPLACE PROCEDURE pr_percent_of_checks_birth(
    INOUT SuccessfulChecks NUMERIC,
    INOUT UnsuccessfulChecks NUMERIC
) AS
$$
DECLARE
    amount INTEGER;
BEGIN
    SELECT COUNT(id)
    INTO amount
    FROM checks
             JOIN peers ON peers.nickname = checks.peer
    WHERE to_char(checks."Date"::DATE, 'mm-dd') =
          to_char(birthday::DATE, 'mm-dd');
    SELECT (SELECT (ROUND(100 * COUNT(*)::NUMERIC / amount, 2))
            FROM (SELECT DISTINCT checks.id
                  FROM checks
                           JOIN P2P p ON p."Check" = Checks.ID AND p.State = 'Success'
                           JOIN Verter ON Checks.ID = Verter."Check" AND Verter.State = 'Success'
                           JOIN peers ON peers.nickname = checks.peer
                  WHERE to_char(checks."Date"::DATE, 'mm-dd') =
                        to_char(birthday::DATE, 'mm-dd')) AS tmp)   AS SuccessfulChecks,
           (SELECT(ROUND(100 * COUNT(*)::NUMERIC / amount, 2))
            FROM (SELECT DISTINCT checks.id, checks.peer, checks.task, 'Failure' AS status
                  FROM checks
                           LEFT OUTER JOIN P2P p ON p."Check" = Checks.ID
                           LEFT OUTER JOIN Verter ON Checks.ID = Verter."Check"
                           LEFT OUTER JOIN peers ON peers.nickname = checks.peer
                  WHERE (p.state = 'Failure'
                      OR verter.state = 'Failure')
                    AND (to_char(checks."Date"::DATE, 'mm-dd') =
                         to_char(birthday::DATE, 'mm-dd'))) AS tmp) AS UnsuccessfulChecks
    INTO SuccessfulChecks,UnsuccessfulChecks;
END;
$$ LANGUAGE plpgsql;

CALL pr_percent_of_checks_birth(0, 0);

-- 3.14
CREATE OR REPLACE PROCEDURE pr_all_checks_one_peer() AS
$$
BEGIN
    CREATE OR REPLACE VIEW local_table_all_checks_one_peer(peer, task, xpamount, r) AS
    (
    SELECT peer, task, xpamount, rank() OVER (PARTITION BY peer, task ORDER BY xpamount DESC)
    FROM checks
             JOIN xp x ON checks.id = x."Check");
END;
$$ LANGUAGE plpgsql;

CALL pr_all_checks_one_peer();

SELECT peer, SUM(xpamount) AS XP
FROM local_table_all_checks_one_peer
WHERE r = 1
GROUP BY peer
ORDER BY XP DESC;

-- 3.15
CREATE
    OR REPLACE PROCEDURE pr_null_third_project(first_project VARCHAR, second_project VARCHAR, third_project VARCHAR,
                                               result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR (SELECT peer
                          FROM checks
                          WHERE task = first_project
                          INTERSECT
                          SELECT peer
                          FROM checks
                          WHERE task = second_project
                          EXCEPT
                          SELECT peer
                          FROM checks
                          WHERE task = third_project);
END;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL pr_null_third_project('SQL3_RetailAnalitycs_v1.0', 'A2_SimpleNavigator_v1.0', 'CPP7_MLP', 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.16
CREATE OR REPLACE PROCEDURE pr_path_to_project(result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        (WITH RECURSIVE task_access_path(Task, PrevCount) AS (SELECT title, 0
                                                              FROM tasks
                                                              WHERE parenttask IS NULL
                                                              UNION ALL
                                                              SELECT title, PrevCount + 1
                                                              FROM task_access_path,
                                                                   tasks
                                                              WHERE task_access_path.Task = tasks.parenttask)
         SELECT Task, PrevCount
         FROM task_access_path);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_path_to_project('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.17
CREATE OR REPLACE FUNCTION fnc_lucky_days(n_day integer)
    RETURNS TABLE
            (
                Date date
            )
AS
$$
DECLARE
    count_success int  := 0;
    prev_d        date := (SELECT MIN("Date")
                           FROM checks);
    value         record;
    today         bool := FALSE;
    l_cur CURSOR FOR (SELECT p.state   AS sstate,
                             c."Date"  AS ddate,
                             p2."Time" AS ttime
                      FROM p2p p
                               JOIN checks c ON c.id = p."Check"
                               JOIN p2p p2 ON c.id = p2."Check" AND p2.state = 'Start'
                      WHERE p.state != 'Start'
                      ORDER BY ddate, ttime);
BEGIN
    FOR value IN l_cur
        LOOP
            IF value.ddate != prev_d THEN
                count_success = 0;
                today = FALSE;
            END IF;
            IF today = TRUE THEN
                prev_d = value.ddate;
                CONTINUE;
            END IF;
            IF value.sstate = 'Success' THEN
                count_success = count_success + 1;
                IF count_success = n_day THEN
                    count_success = 0;
                    Date = value.ddate;
                    today = TRUE;
                    RETURN NEXT;
                END IF;
            ELSE
                count_success = 0;
            END IF;
            prev_d = value.ddate;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_lucky_days(count_checks INTEGER, result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT *
        FROM fnc_lucky_days(count_checks);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_lucky_days(1, 'data');
FETCH ALL IN "data";
COMMIT;
END;

CREATE OR REPLACE PROCEDURE pr_most_success_checks(result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT peer, count(p.state) AS XP
        FROM checks
                 JOIN p2p p ON checks.id = p."Check" AND p.state = 'Success'
        GROUP BY peer
        ORDER BY XP DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_most_success_checks('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.19
CREATE OR REPLACE PROCEDURE pr_most_xp_peer() AS
$$
BEGIN
    CREATE OR REPLACE VIEW local_table_most_xp_peer(peer, task, xpamount, r) AS
    (
    SELECT peer, task, xpamount, rank() OVER (PARTITION BY peer, task ORDER BY xpamount DESC)
    FROM checks
             JOIN xp x ON checks.id = x."Check");
END;
$$ LANGUAGE plpgsql;

CALL pr_most_xp_peer();

SELECT peer, SUM(xpamount) AS XP
FROM local_table_most_xp_peer
WHERE r = 1
GROUP BY peer
ORDER BY XP DESC
LIMIT 1;

-- 3.20
CREATE OR REPLACE FUNCTION fnc_most_overdue_peer()
    RETURNS TABLE
            (
                f_peer             VARCHAR,
                all_time_in_campus TIME
            )
AS
$$
BEGIN
    RETURN QUERY (WITH last_day AS (SELECT id, peer, "Time", state
                                    FROM timetracking
                                    WHERE "Date" = current_date),
                       exit_t AS (SELECT DISTINCT peer,
                                                  "Time"    AS exit,
                                                  (SELECT "Time"
                                                   FROM last_day t2
                                                   WHERE t1.id < t2.id
                                                     AND t2.state = 2
                                                     AND t1.peer = t2.peer
                                                   LIMIT 1) AS login
                                  FROM last_day t1
                                  WHERE t1.state = 1
                                  GROUP BY peer, "Time", t1.id, t1.state),
                       in_campus AS (SELECT peer, (login - exit)::time AS campus_time
                                     FROM exit_t
                                     WHERE (login - exit)::time IS NOT NULL)
                  SELECT peer, SUM(campus_time)::time AS t
                  FROM in_campus
                  GROUP BY peer
                  order by t DESC
                  LIMIT 1);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_most_overdue_peer(result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT f_peer
        FROM fnc_most_overdue_peer();
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_most_overdue_peer('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.21
CREATE OR REPLACE PROCEDURE pr_came_in_early(need_time TIME, need_count INTEGER, result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT peer
        FROM timetracking
        WHERE "Time" < need_time
          AND state = 1
        GROUP BY peer
        HAVING count(state) >= need_count;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_came_in_early('20:00:00', '2', 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.22
CREATE OR REPLACE PROCEDURE pr_often_come_out(number_day INTEGER, number_exit INTEGER, result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT peer
        FROM timetracking
        WHERE "Date" BETWEEN current_date - number_day AND current_date
          AND state = 2
        GROUP BY peer
        HAVING count(state) > number_exit;
END;
$$ LANGUAGE plpgsql;
rollback
BEGIN;
CALL pr_often_come_out(150, 0, 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.23
CREATE OR REPLACE PROCEDURE pr_came_in_last(result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT peer
        FROM timetracking
        WHERE state = 1
          AND "Date" = current_date
        ORDER BY "Time" DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_came_in_last('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.24
CREATE OR REPLACE FUNCTION fnc_out_for_a_while()
    RETURNS TABLE
            (
                f_peer    VARCHAR,
                exit_time TIME
            )
AS
$$
BEGIN
    RETURN QUERY (WITH last_day AS (SELECT id, peer, "Time", state
                                    FROM timetracking
                                    WHERE "Date" = current_date - '1 day'::interval),
                       exit_t AS (SELECT DISTINCT peer,
                                                  "Time"    AS exit,
                                                  (SELECT "Time"
                                                   FROM last_day t2
                                                   WHERE t1.id < t2.id
                                                     AND t2.state = 1
                                                     AND t1.peer = t2.peer
                                                   LIMIT 1) AS login
                                  FROM last_day t1
                                  WHERE t1.state = 2
                                  GROUP BY peer, "Time", t1.id, t1.state)
                  SELECT peer, (login - exit)::time
                  FROM exit_t
                  WHERE (login - exit)::time IS NOT NULL);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_out_for_a_while(number_minutes INTERVAL, result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT DISTINCT f_peer
        FROM fnc_out_for_a_while()
        WHERE exit_time > number_minutes;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_out_for_a_while('10 minutes'::interval, 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.25
CREATE OR REPLACE FUNCTION fnc_tmp_25()
    RETURNS TABLE
            (
                Month        TEXT,
                EarlyEntries NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY (WITH came_on_birthday AS (SELECT DISTINCT nickname, date_part('month', birthday) AS b_month
                                            FROM peers),
                       get_login_month AS (SELECT DISTINCT date_part('month', "Date") AS g_month, peer AS l_p
                                           FROM timetracking),
                       get_number_login_peer_in_month AS (SELECT count(state) AS count_login, peer AS t_peer
                                                          FROM timetracking
                                                          WHERE state = 1
                                                          GROUP BY t_peer),
                       login_in_birth_month AS (SELECT nickname, count_login
                                                FROM came_on_birthday AS c
                                                         JOIN get_login_month ON nickname = l_p AND b_month = g_month
                                                         JOIN get_number_login_peer_in_month g ON g.t_peer = c.nickname),
                       total_login_in_month AS (SELECT sum(count_login) AS total_login FROM login_in_birth_month),--1
                       get_early_login AS (SELECT DISTINCT SUM(state) AS total_early_login, "Date" AS gel_date
                                           FROM login_in_birth_month AS l
                                                    JOIN timetracking t ON t.peer = l.nickname AND t."Time" < '12:00'
                                           GROUP BY gel_date),--2
                       percent_early_login AS (SELECT DISTINCT to_char("Date", 'Month')                AS tmp_m,
                                                               (100 / total_login) * total_early_login AS tmp_p
                                               FROM timetracking
                                                        CROSS JOIN total_login_in_month t
                                                        JOIN get_early_login gel
                                                             ON date_part('month', timetracking."Date") =
                                                                date_part('month', gel.gel_date)
                                                        CROSS JOIN local_table_most_xp_peer)--3
                  SELECT tmp_m, tmp_p
                  FROM percent_early_login);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_login_in_every_month(result_data INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result_data FOR
        SELECT *
        FROM fnc_tmp_25();
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_login_in_every_month('data');
FETCH ALL IN "data";
COMMIT;
END;
