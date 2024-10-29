-- 1) Написать процедуру добавления P2P проверки

CREATE OR REPLACE PROCEDURE insert_p2p_for_task(peer_name TEXT, reviewer TEXT, task_name TEXT, state check_state, timee TIME) AS
$$
BEGIN
        INSERT INTO p2p("check", checking_peer, state, time)
        VALUES ((SELECT max(id) FROM checks), reviewer, state, timee);
END
$$ LANGUAGE plpgsql;

-- 2) Написать процедуру добавления проверки Verter'ом

CREATE OR REPLACE PROCEDURE insert_verter(peer_name TEXT, task_name TEXT, state check_state, timee TIME) AS
$$
DECLARE
    p2p_id INT := (SELECT p2p.id
                   FROM p2p
                      JOIN checks ON p2p."check" = checks.id
                   WHERE peer = peer_name AND task = task_name
                   ORDER BY date DESC
                   LIMIT 1);
BEGIN
    IF ((SELECT count(*) FROM p2p WHERE id = p2p_id AND state = 'success') = 1) THEN
        INSERT INTO verter("check", state, time) VALUES ((SELECT "check" FROM p2p WHERE id = p2p_id), state, timee);
    END IF;
END
$$ LANGUAGE plpgsql;

-- 3) Написать триггер: после добавления записи со статутом "начало" в таблицу P2P,
--    изменить соответствующую запись в таблице TransferredPoints

CREATE OR REPLACE FUNCTION trigger_function_transferred_points() RETURNS TRIGGER AS
$$
DECLARE
    par_peer TEXT := (SELECT peer
                      FROM checks
                      WHERE new."check" = checks.id);
BEGIN
    IF (new.state != 'start') THEN
        RETURN new;
    END IF;
    IF ((SELECT count(*)
         FROM transferred_points
         WHERE checking_peer = new.checking_peer AND checked_peer = par_peer) = 1)
    THEN
        UPDATE transferred_points
        SET points_amount=points_amount + 1
        WHERE checking_peer = new.checking_peer AND checked_peer = par_peer;
    ELSE
        INSERT INTO transferred_points(checking_peer, checked_peer, points_amount)
        VALUES (new.checking_peer, par_peer, 1);
    END IF;
    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER transferred_points_trigger
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function_transferred_points();

-- 4) Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи

CREATE OR REPLACE FUNCTION trigger_function_xp() RETURNS TRIGGER AS
$$
DECLARE
    is_norm INT := (SELECT count(*) FROM p2p
                        JOIN checks ON checks.id = p2p."check"
                        JOIN tasks ON tasks.title = checks.task AND
                                      new.xp_amount <= tasks.max_xp
                    WHERE new."check" = p2p."check" AND state = 'success');
BEGIN
    IF (is_norm = 0) THEN
        RETURN NULL;
    END IF;
    IF (exists(SELECT * FROM checks
                   NATURAL JOIN mas_tasks
               WHERE new."check" = checks.id) AND
        NOT exists (SELECT * FROM checks
                       JOIN verter ON new."check" = verter."check"
                    WHERE state = 'success'))
    THEN
        RETURN NULL;
    END IF;
    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER xp_trigger
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function_xp();
