-- CREATE DATABASE example;

DROP TABLE IF EXISTS abc;
CREATE TABLE abc
(
);
DROP TABLE IF EXISTS abcc;
CREATE TABLE abcc
(
);
DROP TABLE IF EXISTS aabc;
CREATE TABLE aabc
(
);

CREATE OR REPLACE FUNCTION trigger_procedure1() RETURNS TRIGGER AS
$$
BEGIN
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_1
    BEFORE INSERT
    ON abc
    FOR EACH ROW
EXECUTE PROCEDURE trigger_procedure1();

CREATE OR REPLACE FUNCTION trigger_procedure2() RETURNS TRIGGER AS
$$
BEGIN
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_2
    BEFORE INSERT
    ON abcc
    FOR EACH ROW
EXECUTE PROCEDURE trigger_procedure2();

CREATE OR REPLACE FUNCTION trigger_procedure3() RETURNS TRIGGER AS
$$
BEGIN
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_3
    BEFORE INSERT
    ON aabc
    FOR EACH ROW
EXECUTE PROCEDURE trigger_procedure3();

-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает все те таблицы текущей базы данных, имена которых начинаются с фразы 'TableName'.

CREATE OR REPLACE PROCEDURE destroy_tables(par_table_name TEXT) AS
$$
DECLARE
    name TEXT;
BEGIN
    FOR name IN SELECT information_schema.tables.table_name
                FROM information_schema.tables
                WHERE table_name LIKE par_table_name || '%'
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(name) || ' CASCADE';
        END LOOP;
END;
$$ LANGUAGE plpgsql;


CALL destroy_tables('abc');

-- 2) Создать хранимую процедуру с выходным параметром, которая выводит список имен и параметров всех скалярных SQL функций пользователя в текущей базе данных. Имена функций без параметров не выводить. Имена и список параметров должны выводиться в одну строку. Выходной параметр возвращает количество найденных функций.

CREATE OR REPLACE PROCEDURE triggers_name(OUT trigger_name_count INT) AS
$$
DECLARE
    trigger_record RECORD;
BEGIN
    trigger_name_count := 0;
    FOR trigger_record IN
        (SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
         FROM information_schema.routines
         LEFT JOIN information_schema.parameters ON routines.specific_name = parameters.specific_name
         WHERE routines.specific_schema = current_schema() AND parameters.data_type IS NOT NULL
         ORDER BY routines.routine_name, parameters.ordinal_position)
        LOOP
            RAISE NOTICE 'function: %(%)', trigger_record.routine_name, trigger_record.data_type;
            trigger_name_count := trigger_name_count + 1;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

DO
$$
    DECLARE
        trigger_count INT;
    BEGIN
        CALL triggers_name(trigger_count);
        RAISE NOTICE 'Functions found %', trigger_count;
    END
$$;

-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных. Выходной параметр возвращает количество уничтоженных триггеров.

CREATE OR REPLACE PROCEDURE destroy_triggers(OUT destroyed_trigger_count INT) AS
$$
DECLARE
    trigger_record RECORD ;
BEGIN
    destroyed_trigger_count := 0;
    FOR trigger_record IN (SELECT trigger_name, event_object_table
                           FROM information_schema.triggers
                           WHERE trigger_schema = current_schema())
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(trigger_record.trigger_name) || ' ON ' ||
                    quote_ident(trigger_record.event_object_table);
            destroyed_trigger_count := destroyed_trigger_count + 1;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

DO
$$
    DECLARE
        trigger_count INT;
    BEGIN
        CALL destroy_triggers(trigger_count);
        RAISE NOTICE 'Destroyed % triggers', trigger_count;
    END
$$;

-- 4) Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов (только хранимых процедур и скалярных функций), в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры.

CREATE OR REPLACE PROCEDURE triggers_name_with_found_command(par_table_name TEXT) AS
$$
DECLARE
    trigger_record RECORD ;
BEGIN
    FOR trigger_record IN
        (SELECT routines.routine_name, routines.routine_type
         FROM information_schema.routines
         LEFT JOIN information_schema.parameters ON routines.specific_name = parameters.specific_name
         WHERE routines.specific_schema = current_schema() AND
               routine_definition ILIKE '%' || par_table_name || '%'
         ORDER BY routines.routine_name, parameters.ordinal_position)
        LOOP
            RAISE NOTICE 'function: %, %', trigger_record.routine_name, trigger_record.routine_type;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

DO
$$
    BEGIN
        CALL triggers_name_with_found_command('LOOP');
    END
$$;
