CREATE TABLE Peers
(
    Nickname VARCHAR PRIMARY KEY,
    Birthday DATE NOT NULL
);

INSERT INTO Peers (nickname, birthday)
VALUES ('hspeaker', '2000-10-09'),
       ('bfile', '2001-02-10'),
       ('bromanyt', '2002-03-11'),
       ('jcraster', '2003-04-12'),
       ('adough', '2004-11-13'),
       ('werewolf', '2000-06-14'),
       ('mzoraida', '2002-08-16'),
       ('chmackey', '2004-10-18');

CREATE TABLE Tasks
(
    Title      VARCHAR PRIMARY KEY,
    ParentTask VARCHAR NULL,
    MaxXp      INTEGER DEFAULT 0,

    CONSTRAINT fk_Tasks FOREIGN KEY (ParentTask) REFERENCES Tasks (Title)
);

CREATE UNIQUE INDEX i_null_tasks ON Tasks ((Tasks.ParentTask IS NULL)) WHERE Tasks.ParentTask IS NULL;

ALTER TABLE Tasks
    ADD CONSTRAINT ch_tasks_maxxp
        CHECK (MaxXP >= 0);

INSERT INTO Tasks(Title, MaxXp)
VALUES ('CPP4_3DViewer_v2.0', 670);

INSERT INTO Tasks(Title, ParentTask, MaxXp)
VALUES ('CPP7_MLP', 'CPP4_3DViewer_v2.0', 670),
       ('A1_Maze', 'CPP4_3DViewer_v2.0', 780),
       ('SQL1', 'CPP4_3DViewer_v2.0', 1500),
       ('A2_SimpleNavigator_v1.0', 'A1_Maze', 900),
       ('A3_Parallels', 'A2_SimpleNavigator_v1.0', 550);

CREATE TABLE Checks
(
    ID     SERIAL PRIMARY KEY,
    Peer   VARCHAR,
    Task   VARCHAR,
    "Date" DATE DEFAULT current_date,

    CONSTRAINT fk_Checks_Task FOREIGN KEY (Task) REFERENCES Tasks (Title),
    CONSTRAINT fk_Checks_Peer FOREIGN KEY (Peer) REFERENCES Peers (nickname)
);

CREATE TYPE "check_status" AS ENUM (
    'Start',
    'Success',
    'Failure'
    );

INSERT INTO Checks(Peer, Task, "Date")
VALUES ('bfile', 'A1_Maze', '2022-10-08'),
       ('hspeaker', 'SQL1', '2022-10-09'),
       ('jcraster', 'CPP7_MLP', '2022-10-10'),
       ('werewolf', 'SQL1', '2022-10-09');


CREATE TABLE P2P
(
    ID           SERIAL PRIMARY KEY,
    "Check"      BIGINT  NOT NULL,
    CheckingPeer VARCHAR NOT NULL,
    State        check_status,
    "Time"       TIME DEFAULT current_time,

    CONSTRAINT fk_P2P_check_ID FOREIGN KEY ("Check") REFERENCES Checks (ID),
    CONSTRAINT fk_P2P_checking_Peer FOREIGN KEY (CheckingPeer) REFERENCES Peers (nickname)
);

INSERT INTO P2P("Check", CheckingPeer, State, "Time")
VALUES (1, 'jcraster', 'Start', '14:53'),
       (1, 'jcraster', 'Failure', '15:15'),
       (4, 'bromanyt', 'Start', '15:01'),
       (4, 'bromanyt', 'Success', '15:35'),
       (2, 'werewolf', 'Start', '16:00'),
       (2, 'werewolf', 'Success', '16:15'),
       (3, 'hspeaker', 'Start', '15:15'),
       (3, 'hspeaker', 'Success', '16:15');

CREATE TABLE Verter
(
    ID      SERIAL PRIMARY KEY,
    "Check" BIGINT NOT NULL,
    State   check_status,
    "Time"  TIME DEFAULT current_time,

    CONSTRAINT fk_Verter FOREIGN KEY ("Check") REFERENCES Checks (ID)
);

INSERT INTO Verter("Check", State, "Time")
VALUES (4, 'Start', '15:36'),
       (4, 'Failure', '15:50'),
       (2, 'Start', '16:16'),
       (2, 'Success', '16:40'),
       (3, 'Start', '15:21'),
       (3, 'Success', '15:43');

CREATE TABLE TransferredPoints
(
    ID           SERIAL PRIMARY KEY,
    CheckingPeer VARCHAR NOT NULL,
    CheckedPeer  VARCHAR NOT NULL,
    PointsAmount INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT fk_TransferredPoints_CheckingPeer FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_TransferredPoints_CheckedPeer FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname)
);

WITH get_peers_name AS (SELECT DISTINCT CheckingPeer AS checking, C2.Peer AS checked, COUNT(DISTINCT "Check") AS counter
                        FROM P2P
                                 JOIN Checks C2 ON C2.ID = P2P."Check"
                        GROUP BY checking, checked)

INSERT
INTO TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount)
SELECT checking, checked, counter
FROM get_peers_name;

CREATE TABLE Friends
(
    ID    SERIAL PRIMARY KEY,
    Peer1 VARCHAR NOT NULL,
    Peer2 VARCHAR NOT NULL,

    CONSTRAINT fk_Friends_Peer1 FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
    CONSTRAINT fk_Friends_Peer2 FOREIGN KEY (Peer2) REFERENCES Peers (Nickname)
);

INSERT INTO Friends(Peer1, Peer2)
VALUES ('hspeaker', 'bfile'),
       ('bfile', 'hspeaker'),
       ('werewolf', 'adough'),
       ('adough', 'werewolf'),
       ('bromanyt', 'bfile'),
       ('bfile', 'bromanyt'),
       ('mzoraida', 'chmackey'),
       ('chmackey', 'mzoraida');

CREATE TABLE Recommendations
(
    ID              SERIAL PRIMARY KEY,
    Peer            VARCHAR NOT NULL,
    RecommendedPeer VARCHAR NOT NULL,

    CONSTRAINT fk_Recommendations_Peer FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_Recommendations_RecommendedPeer FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname)
);

WITH check_p2p AS (SELECT CheckingPeer AS checking_peer, "Check" AS checked_peer FROM P2P WHERE State = 'Success')

INSERT
INTO Recommendations(Peer, RecommendedPeer)
SELECT Checks.Peer, checking_peer
FROM Checks
         JOIN check_p2p c ON c.checked_peer = Checks.ID;

CREATE TABLE XP
(
    ID       SERIAL PRIMARY KEY,
    "Check"  BIGINT  NOT NULL,
    XPAmount INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT fk_XP_Check FOREIGN KEY ("Check") REFERENCES Checks (ID)
);

ALTER TABLE XP
    ADD CONSTRAINT ch_xp_xpamount
        CHECK (XPAmount >= 0);

WITH tmp_checks AS (SELECT Checks.ID AS checks_id, Checks.Task AS checks_task
                    FROM Checks
                             JOIN P2P p ON p."Check" = Checks.ID AND p.State = 'Success'
                    EXCEPT
                    SELECT Checks.ID AS checks_id, Checks.Task AS checks_task
                    FROM Checks
                             JOIN Verter V ON Checks.ID = V."Check" AND v.state = 'Failure'),
     tmp_tasks_xp AS (SELECT DISTINCT MaxXp AS tasks_xp, Tasks.Title AS tasks_title
                      FROM Tasks
                               JOIN Checks ON Tasks.Title = Checks.Task
                               JOIN P2P p ON p."Check" = Checks.ID AND p.State = 'Success')

INSERT
INTO XP("Check", XPAmount)
SELECT DISTINCT checks_id, tasks_xp
FROM tmp_checks
         JOIN tmp_tasks_xp ON tasks_title = checks_task;

CREATE
    OR REPLACE FUNCTION is_entry(status integer, name varchar, day date, period time)
    RETURNS boolean AS
$$
BEGIN
    IF
        (status = 1) THEN
        IF ((SELECT MAX(id) FROM timetracking WHERE peer = name AND "Date" = day) IS NULL) THEN
            RETURN TRUE;
        ELSE
            IF (SELECT state
                FROM timetracking
                WHERE id = (SELECT MAX(id) FROM timetracking WHERE peer = name AND "Date" = day)
                  AND "Time" < period) = 2 THEN
                RETURN TRUE;
            ELSE
                RETURN FALSE;
            END IF;
        END IF;
    ELSE
        IF (SELECT state
            FROM timetracking
            WHERE id = (SELECT MAX(id) FROM timetracking WHERE peer = name AND "Date" = day)
              AND "Time" < period) = 1 THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;
    END IF;
END;
$$
    LANGUAGE plpgsql STABLE
                     STRICT;

CREATE TABLE TimeTracking
(
    ID     SERIAL PRIMARY KEY,
    Peer   VARCHAR NOT NULL,
    "Date" DATE    NOT NULL DEFAULT current_date,
    "Time" TIME             DEFAULT current_time,
    State  INTEGER NOT NULL CHECK ( is_entry(state, Peer, "Date", "Time") IS TRUE ),

    CONSTRAINT fk_TimeTracking_Peer FOREIGN KEY (Peer) REFERENCES Peers (Nickname)
);

ALTER TABLE TimeTracking
    ADD CONSTRAINT ch_state check ( State between 1 AND 2);

INSERT INTO TimeTracking(peer, "Date", "Time", state)
VALUES ('bfile', '2022-02-18', '11:09', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('bfile', '2022-02-18', '17:00', 2);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('jcraster', '2022-04-18', '17:00', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('jcraster', '2022-04-18', '17:03', 2);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('bromanyt', '2022-03-03', '10:42', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('bromanyt', '2022-03-03', '13:42', 2);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('bromanyt', '2022-03-03', '16:42', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('bromanyt', '2022-03-03', '22:42', 2);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('mzoraida', '2022-08-04', '12:13', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('mzoraida', '2022-08-04', '14:15', 2);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('hspeaker', '2022-10-11', '10:53', 1),
       ('bfile', '2022-10-11', '11:23', 1),
       ('bromanyt', '2022-10-11', '12:21', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('hspeaker', '2022-10-11', '18:53', 2),
       ('bromanyt', '2022-10-11', '19:53', 2);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('bromanyt', '2022-10-11', '20:00', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('bromanyt', '2022-10-11', '21:03', 2),
       ('bfile', '2022-10-11', '12:23', 2);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('adough', '2022-11-18', '17:00', 1);
INSERT INTO TimeTracking(Peer, "Date", "Time", State)
VALUES ('adough', '2022-11-18', '17:37', 2);
INSERT INTO TimeTracking(peer, "Time", state)
VALUES ('adough', '10:00', 1),
       ('chmackey', '11:13', 1);
INSERT INTO TimeTracking(peer, "Time", state)
VALUES ('adough', '13:13', 2),
       ('chmackey', '15:00', 2);

CREATE OR REPLACE PROCEDURE import_from_csv(directory text) AS
$$
DECLARE
    str text;
BEGIN
    str:= 'copy Peers(Nickname, Birthday) FROM ''' || directory || '/peer.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Tasks(Title, ParentTask, MaxXp) FROM ''' || directory || '/tasks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Checks(Peer, Task, "Date") FROM ''' || directory || '/checks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy P2P("Check", CheckingPeer, State, "Time") FROM ''' || directory ||
           '/p2p.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Verter("Check", State, "Time") FROM ''' || directory || '/verter.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount) FROM ''' || directory ||
           '/transferredpoints.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Friends(Peer1, Peer2) FROM ''' || directory || '/friends.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Recommendations(Peer, RecommendedPeer) FROM ''' || directory ||
           '/recommendations.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy XP("Check", XPAmount) FROM ''' || directory || '/xp.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
END;
$$
    LANGUAGE plpgsql;

CALL import_from_csv('/Users/bfile/Projects/SQL2_Info21_v1.0-0/src/csv');

CREATE OR REPLACE PROCEDURE export_to_csv(directory text) AS
$$
DECLARE
    str text;
BEGIN
    str:= 'copy Peers(Nickname, Birthday) TO ''' || directory || '/peer.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Tasks(Title, ParentTask, MaxXp) TO ''' || directory || '/tasks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Checks(Peer, Task, "Date") TO ''' || directory || '/checks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy P2P("Check", CheckingPeer, State, "Time") TO ''' || directory ||
           '/p2p.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Verter("Check", State, "Time") TO ''' || directory || '/verter.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount) TO ''' || directory ||
           '/transferredpoints.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Friends(Peer1, Peer2) TO ''' || directory || '/friends.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy Recommendations(Peer, RecommendedPeer) TO ''' || directory ||
           '/recommendations.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:= 'copy XP("Check", XPAmount) TO ''' || directory || '/xp.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
END;
$$
    LANGUAGE plpgsql;

CALL export_to_csv('/Users/bfile/Projects/SQL2_Info21_v1.0-0/src/csv');

-- DROP TABLE IF EXISTS p2p CASCADE;
-- DROP TABLE IF EXISTS checks CASCADE;
-- DROP TABLE IF EXISTS timetracking CASCADE;
-- DROP TABLE IF EXISTS transferredpoints CASCADE;
-- DROP TABLE IF EXISTS Verter CASCADE;
-- DROP TABLE IF EXISTS friends CASCADE;
-- DROP TABLE IF EXISTS peers CASCADE;
-- DROP TABLE IF EXISTS recommendations CASCADE;
-- DROP TABLE IF EXISTS tasks CASCADE;
-- DROP TABLE IF EXISTS xp CASCADE;
-- DROP ROUTINE IF EXISTS import_from_csv(directory text);
-- DROP ROUTINE IF EXISTS export_to_csv(directory text);
-- DROP ROUTINE IF EXISTS is_entry(status integer, name varchar, day date, period time);
-- drop type check_status cascade;
