CREATE DATABASE part4;
-- \connect part4;

CREATE TABLE TableName123
(
    ID     SERIAL PRIMARY KEY,
    Naming VARCHAR
);

CREATE TABLE NeTableName123
(
    ID     SERIAL PRIMARY KEY,
    Naming VARCHAR
);

CREATE TABLE TableNames
(
    ID     SERIAL PRIMARY KEY,
    Naming VARCHAR
);

CREATE TABLE TableNaming
(
    ID     SERIAL PRIMARY KEY,
    Naming VARCHAR
);

CREATE TABLE TTableName
(
    ID     SERIAL PRIMARY KEY,
    Naming VARCHAR
);

CREATE TABLE TableName
(
    ID     SERIAL PRIMARY KEY,
    Naming VARCHAR
);

CREATE OR REPLACE FUNCTION func_1()
    RETURNS INTEGER
AS
$$
DECLARE
    amount INTEGER;
BEGIN
    SELECT count(*)
    INTO amount
    FROM TableName123;

    RETURN amount;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_2(a INTEGER, b INTEGER)
    RETURNS INTEGER
AS
$$
DECLARE
    amount INTEGER;
BEGIN
    SELECT a + b
    INTO amount;

    RETURN amount;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_3(prefix VARCHAR)
    RETURNS VARCHAR
AS
$$
DECLARE
    name VARCHAR;
BEGIN
    SELECT prefix
    INTO name;

    RETURN name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_4(prefix VARCHAR, num INTEGER)
    RETURNS NUMERIC
AS
$$
DECLARE
    res NUMERIC;
BEGIN
    SELECT num::NUMERIC
    INTO res
    FROM TableName123
    WHERE Naming = ('' || prefix || '');

    RETURN res;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION trigger_function()
    RETURNS TRIGGER
AS
$$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_name
    AFTER INSERT
    ON TableNaming
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function();

CREATE OR REPLACE FUNCTION trigger_function2()
    RETURNS TRIGGER
AS
$$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_name2
    BEFORE INSERT
    ON TableNaming
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function2();

-- 1
CREATE OR REPLACE PROCEDURE pr_drop_table(in_prefix text) AS
$$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT quote_ident(table_schema) || '.'
                   || quote_ident(table_name)
        FROM information_schema.tables
        WHERE table_name LIKE ('' || in_prefix || '') || '%'
          AND table_schema NOT LIKE 'pg\_%'
        LOOP
            EXECUTE 'DROP TABLE ' || tbl;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

call pr_drop_table('tablename');

-- 2
CREATE OR REPLACE PROCEDURE pr_function_information(OUT out_amount INTEGER)
AS
$$
DECLARE
    pr_cursor CURSOR FOR
        SELECT routine_name || '; ' || STRING_AGG(p.parameter_name, ', ') || '; ' || STRING_AGG(p.data_type, ', ')
        FROM information_schema.routines r
                 JOIN information_schema.parameters p ON r.specific_name = p.specific_name
        WHERE r.specific_schema = 'public'
          AND r.routine_type = 'FUNCTION'
        GROUP BY r.routine_name
        ORDER BY routine_name;
    DECLARE data VARCHAR;
BEGIN
    out_amount := 0;
    OPEN pr_cursor;
    LOOP
        FETCH pr_cursor INTO data;
        EXIT WHEN NOT FOUND;
        RAISE INFO '%', data;
        out_amount := out_amount + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DO
$$
    DECLARE
        number INTEGER;
    BEGIN
        CALL pr_function_information(number);
        RAISE INFO 'Total functions: %', number;
    END;
$$;

-- 3
CREATE OR REPLACE PROCEDURE pr_drop_triggers(OUT out_amount INTEGER)
AS
$$
DECLARE
    statement TEXT;
    DECLARE i RECORD;
BEGIN
    out_amount := 0;
    FOR i IN (SELECT trigger_name, event_object_table
              FROM information_schema.triggers
              WHERE trigger_schema = 'public')
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || i.trigger_name || ' ON ' || i.event_object_table || ' CASCADE;';
            out_amount = out_amount + 1;
        END LOOP;
END ;
$$ LANGUAGE plpgsql;

DO
$$
    DECLARE
        number INTEGER DEFAULT 0;
    BEGIN
        CALL pr_drop_triggers(number);
        RAISE INFO 'Amount of dropped triggers: %', number;
    END ;
$$;

-- 4
CREATE OR REPLACE PROCEDURE fn_search_prefix(IN in_prefix VARCHAR, INOUT inout_result VARCHAR)
AS
$$
DECLARE
    list   VARCHAR[];
    object VARCHAR;
    i      BIGINT DEFAULT 1;
BEGIN
    in_prefix := '%' || in_prefix || '%';
    list := ARRAY(SELECT proname
                  FROM pg_proc
                           INNER JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                  WHERE nspname = 'public'
                    AND prokind IN ('f', 'p')
                    AND prosrc LIKE in_prefix);
    FOREACH object IN ARRAY list
        LOOP
            IF (SELECT prokind
                FROM pg_proc
                         INNER JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE proname = quote_ident(object)) = 'f' THEN
                list[i] := list[i] || ' ' || '(function)';
            ELSE
                list[i] := list[i] || ' ' || '(procedure)';
            END IF;
            i := i + 1;
        END LOOP;
    inout_result := array_to_string(list, ', ');
END;
$$ LANGUAGE plpgsql;

CALL fn_search_prefix('SELECT', NULL);
CALL fn_search_prefix('FROM information_schema.tables', NULL);

DROP TABLE IF EXISTS TableName123;
DROP TABLE IF EXISTS NeTableName123;
DROP TABLE IF EXISTS TableNames;
DROP TABLE IF EXISTS TableNaming;
DROP TABLE IF EXISTS TTableName;
DROP TABLE IF EXISTS TableName;
DROP ROUTINE IF EXISTS func_1();
DROP ROUTINE IF EXISTS func_2(a INTEGER, b INTEGER);
DROP ROUTINE IF EXISTS func_3(prefix VARCHAR);
DROP ROUTINE IF EXISTS func_4(prefix VARCHAR, num INTEGER);
DROP ROUTINE IF EXISTS pr_drop_table(prefix text);
DROP ROUTINE IF EXISTS pr_function_information(OUT number INTEGER);
DROP ROUTINE IF EXISTS trigger_function();
DROP ROUTINE IF EXISTS trigger_function2();
DROP ROUTINE IF EXISTS pr_drop_triggers(OUT amount INTEGER);
DROP ROUTINE IF EXISTS fn_search_prefix(IN prefix VARCHAR, INOUT result VARCHAR);
