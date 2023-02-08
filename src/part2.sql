CREATE OR REPLACE FUNCTION fnc_random_xp(max_xp INTEGER) RETURNS INTEGER AS
$$
DECLARE
    result_xp     INTEGER;
    result_xp_min INTEGER;
BEGIN
    result_xp_min = (max_xp * 0.8);
    result_xp = (SELECT i::INTEGER FROM generate_series(result_xp_min, max_xp) s(i) ORDER BY RANDOM() LIMIT 1);
    RETURN result_xp;
END;
$$ LANGUAGE plpgsql;

-- part 2
CREATE OR REPLACE PROCEDURE pr_add_verter(checked_peer VARCHAR, added_task VARCHAR, added_state check_status,
                                       added_time TIME) AS
$$
BEGIN
    INSERT INTO verter("Check", state, "Time")
    SELECT "Check", 'Start', "Time"
    FROM p2p
             JOIN checks c ON c.id = p2p."Check" AND c.peer = checked_peer
    WHERE state = 'Success'
      AND c.task = added_task
    ORDER BY "Check"
    LIMIT 1;
    IF (added_state = 'Success') THEN
        INSERT INTO xp("Check", xpamount)
        VALUES ((SELECT MAX(id) FROM checks), fnc_random_xp((SELECT maxxp FROM tasks WHERE title = added_task)));
    END IF;
    INSERT INTO verter("Check", state, "Time")
    VALUES ((SELECT id FROM checks WHERE peer = checked_peer AND task = added_task ORDER BY id DESC LIMIT 1),
            added_state, added_time);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_random_result() RETURNS check_status AS
$$
DECLARE
    result_state check_status;
BEGIN
    result_state = (SELECT state
                    FROM p2p
                    WHERE state = 'Success'
                       OR state = 'Failure'
                    ORDER BY RANDOM()
                    LIMIT 1);
    RETURN result_state;
END;
$$ LANGUAGE plpgsql;

--part 1
CREATE OR REPLACE PROCEDURE pr_add_p2p(checked_peer VARCHAR, checking_peer VARCHAR, added_task VARCHAR,
                                    added_state check_status, added_time TIME) AS
$$
DECLARE
    checked_id    INTEGER;
    checked_exist INTEGER;
    count_check   INTEGER;
BEGIN
    checked_exist = (SELECT count(p."Check")
                     FROM checks
                              JOIN p2p p ON checks.id = p."Check" AND p.checkingpeer = checking_peer
                     WHERE peer = checked_peer
                       AND Task = added_task);
    count_check = (SELECT count(p."Check")
                   FROM checks
                            JOIN p2p p ON checks.id = p."Check" AND p.checkingpeer = checking_peer
                   WHERE peer = checked_peer
                     AND state = 'Start' AND task = added_task);
    RAISE NOTICE 'Calling cs_create_job(%)', count_check;
    CASE
        WHEN checked_exist > 1 THEN RAISE EXCEPTION 'There can only be one project check!';
        WHEN added_state = 'Start' AND count_check = 1 THEN RAISE EXCEPTION 'The last check is not over!';
        WHEN added_state = 'Start' AND checked_exist = 0 THEN INSERT INTO Checks(Peer, Task)
                                                              VALUES (checked_peer, added_task);
                                                              INSERT INTO P2P("Check", CheckingPeer, State, "Time")
                                                              VALUES ((SELECT MAX(ID) FROM Checks), checking_peer,
                                                                      added_state, added_time);
        ELSE checked_id =
                     (SELECT c.id
                      FROM p2p
                               JOIN checks C ON c.peer = checked_peer AND c.task = added_task
                      WHERE p2p.checkingpeer = checking_peer
                      ORDER BY "Check"
                      LIMIT 1);
             INSERT
             INTO P2P("Check", CheckingPeer, State, "Time")
             VALUES (checked_id, checking_peer, added_state, added_time);
             IF added_state = 'Success' THEN
                 CALL pr_add_verter(checked_peer, added_task, fnc_random_result(), added_time + '3 minutes');
             END IF;
        END CASE;
END
$$ LANGUAGE plpgsql;

-- part 3
CREATE OR REPLACE FUNCTION fnc_change_transferred_points_after_p2p_start() RETURNS TRIGGER AS
$$
DECLARE
    tmp_checked_peer  VARCHAR;
    tmp_checking_peer VARCHAR;
    tmp_checked_id    INTEGER;
BEGIN
    IF (TG_OP = 'INSERT' AND NEW.state = 'Start') THEN
        tmp_checking_peer =
                (SELECT t.checkingpeer
                 FROM transferredpoints t
                          JOIN p2p p ON NEW.checkingpeer = p.checkingpeer AND NEW."Check" = p."Check"
                          JOIN checks c2 ON c2.id = p."Check" AND checkedpeer = c2.peer
                 WHERE t.checkingpeer = NEW.checkingpeer);
        tmp_checked_peer = (SELECT peer FROM checks WHERE checks.ID = NEW."Check");
        tmp_checked_id = (SELECT id FROM checks WHERE id = NEW."Check");
        CASE
            WHEN(tmp_checking_peer = NEW.checkingpeer IS NULL OR
                 NEW."Check" = tmp_checked_id IS NULL)
                THEN INSERT INTO transferredpoints(checkingpeer, checkedpeer, pointsamount)
                     SELECT checkingpeer, c.peer, COUNT(DISTINCT "Check")
                     FROM p2p
                              JOIN checks c ON c.id = p2p."Check"
                     WHERE p2p.id = (SELECT MAX(ID) FROM p2p)
                     GROUP BY checkingpeer, peer;
            ELSE UPDATE transferredpoints
                 SET PointsAmount = PointsAmount + 1
                 WHERE transferredpoints.CheckingPeer = tmp_checking_peer
                   AND transferredpoints.CheckedPeer = tmp_checked_peer;
            END CASE;
    END IF;
    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_change_transferred_points_after_p2p_start
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE fnc_change_transferred_points_after_p2p_start();

--part 4
CREATE OR REPLACE FUNCTION fnc_checking_xp() RETURNS TRIGGER AS
$$
DECLARE
    tmp_xp       INTEGER;
    tmp_stat     VARCHAR;
    tmp_checks   VARCHAR;
    tmp_tasks_xp VARCHAR;

BEGIN
    tmp_xp = (SELECT tasks.maxxp
              FROM tasks
                       JOIN checks c ON tasks.title = c.task
              WHERE c.id = NEW."Check");
    tmp_stat = (SELECT p.state
                FROM Checks
                         JOIN P2P p ON p."Check" = NEW."Check" AND p.state = 'Success'
                         JOIN Verter ON Verter."Check" = Checks.id AND verter.state = 'Success'
                LIMIT 1); -- думаю, нужно заменить, на то, что в part1
    IF (TG_OP = 'INSERT' AND NEW.xpamount <= tmp_xp AND tmp_stat IS NOT NULL) THEN
        RETURN NEW;
    ELSE
        RETURN NULL;
    END IF;
END
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER trg_checking_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE fnc_checking_xp();

CREATE OR REPLACE FUNCTION fnc_delete_from_transferredpoints() RETURNS TRIGGER AS
$$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        PERFORM setval(pg_get_serial_sequence('transferredpoints', 'id')
            , COALESCE(max(id) + 1, 1)
            , false)
        FROM transferredpoints;
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER trg_delete_from_transferredpoints
    AFTER DELETE
    ON transferredpoints
    FOR EACH ROW
EXECUTE PROCEDURE fnc_delete_from_transferredpoints();

CREATE OR REPLACE FUNCTION fnc_delete_from_xp() RETURNS TRIGGER AS
$$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        PERFORM setval(pg_get_serial_sequence('xp', 'id')
            , COALESCE(max(id) + 1, 1)
            , false)
        FROM xp;
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER trg_delete_from_xp
    AFTER DELETE
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE fnc_delete_from_xp();

CREATE OR REPLACE FUNCTION fnc_delete_from_p2p() RETURNS TRIGGER AS
$$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        PERFORM setval(pg_get_serial_sequence('p2p', 'id')
            , COALESCE(max(id) + 1, 1)
            , false)
        FROM p2p;
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER trg_delete_from_p2p
    AFTER DELETE
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE fnc_delete_from_p2p();

CREATE OR REPLACE FUNCTION fnc_delete_from_checks() RETURNS TRIGGER AS
$$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        PERFORM setval(pg_get_serial_sequence('checks', 'id')
            , COALESCE(max(id) + 1, 1)
            , false)
        FROM checks;
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER trg_delete_from_checks
    AFTER DELETE
    ON checks
    FOR EACH ROW
EXECUTE PROCEDURE fnc_delete_from_checks();

CREATE OR REPLACE FUNCTION fnc_delete_from_verter() RETURNS TRIGGER AS
$$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        PERFORM setval(pg_get_serial_sequence('verter', 'id')
            , COALESCE(max(id) + 1, 1)
            , false)
        FROM verter;
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER trg_delete_from_verter
    AFTER DELETE
    ON verter
    FOR EACH ROW
EXECUTE PROCEDURE fnc_delete_from_verter();


CALL pr_add_p2p('werewolf', 'jcraster', 'A2_SimpleNavigator_v1.0', 'Start', '08:24');
CALL pr_add_p2p('werewolf', 'jcraster', 'A2_SimpleNavigator_v1.0', 'Success', '08:26');
CALL pr_add_p2p('bromanyt', 'mzoraida', 'CPP7_MLP', 'Start', '18:24');
CALL pr_add_p2p('bromanyt', 'mzoraida', 'CPP7_MLP', 'Success', '18:26');
CALL pr_add_p2p('werewolf', 'jcraster', 'CPP7_MLP', 'Start', '23:27');
CALL pr_add_p2p('werewolf', 'jcraster', 'CPP7_MLP', 'Success', '23:30');
CALL pr_add_p2p('mzoraida', 'adough', 'CPP7_MLP', 'Start', '23:33');
CALL pr_add_p2p('mzoraida', 'adough', 'CPP7_MLP', 'Success', '23:37');
