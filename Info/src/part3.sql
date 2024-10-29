-- ##### 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
-- Ник пира 1, ник пира 2, количество переданных пир поинтов. \
-- Количество отрицательное, если пир 2 получил от пира 1 больше поинтов.

CREATE OR REPLACE FUNCTION transferred_points_human_readable()
    RETURNS TABLE
            (
                Peer1        TEXT,
                Peer2        TEXT,
                Points_Amount INT
            )
AS
$$
SELECT transferred_points.checking_peer AS peer1,
       transferred_points.checked_peer AS peer2,
       coalesce(transferred_points.points_amount, 0) - coalesce(a.points_amount, 0)
FROM transferred_points
LEFT JOIN transferred_points AS a ON a.checking_peer = transferred_points.checked_peer AND
                                     a.checked_peer = transferred_points.checking_peer AND
                                     transferred_points.checking_peer < transferred_points.checked_peer
ORDER BY 1, 2;
$$ LANGUAGE sql;

SELECT *
FROM transferred_points_human_readable();
-- WHERE Peer1 = 'dotharer';
-- SELECT *
-- FROM transferred_points
-- WHERE checking_peer = 'dotharer';

-- ##### 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
-- В таблицу включать только задания, успешно прошедшие проверку (определять по таблице Checks). \
-- Одна задача может быть успешно выполнена несколько раз. В таком случае в таблицу включать все успешные проверки.

CREATE OR REPLACE FUNCTION success_checks()
    RETURNS TABLE
            (
                Peer TEXT,
                Task TEXT,
                XP   INT
            )
AS
$$
SELECT checks.peer, checks.task, xp.xp_amount
FROM checks
    JOIN xp ON xp."check" = checks.id
ORDER BY 1, 2, 3;
$$ LANGUAGE sql;

SELECT *
FROM success_checks();

-- ##### 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
-- Параметры функции: день, например 12.05.2022. \
-- Функция возвращает только список пиров.

CREATE OR REPLACE FUNCTION no_exit(day DATE)
    RETURNS TABLE
            (
                Peer TEXT
            )
AS
$$
SELECT peer
FROM (SELECT peer, date, count(*)
      FROM time_tracking
      WHERE date = day
      GROUP BY peer, date
      HAVING count(*) = 2) AS a
WHERE a.date = day
ORDER BY 1;
$$ LANGUAGE sql;

SELECT *
FROM no_exit((SELECT time_tracking.date
                   FROM time_tracking
                   ORDER BY random()
                   LIMIT 1));

-- ##### 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
-- Результат вывести отсортированным по изменению числа поинтов. \
-- Формат вывода: ник пира, изменение в количество пир поинтов

CREATE OR REPLACE PROCEDURE peerpoint_balance(result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        SELECT a.checking_peer AS peer, coalesce(a.count, 0) - coalesce(b.count, 0) AS points_change
        FROM (SELECT checking_peer, count(*) FROM transferred_points GROUP BY checking_peer) AS a
                FULL JOIN
             (SELECT checked_peer, count(*) FROM transferred_points GROUP BY checked_peer) AS b
                ON a.checking_peer = b.checked_peer
        ORDER BY 2 DESC, 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL peerpoint_balance('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой [первой функцией из Part 3](#1-написать-функцию-возвращающую-таблицу-transferredpoints-в-более-человекочитаемом-виде)
-- Результат вывести отсортированным по изменению числа поинтов. \
-- Формат вывода: ник пира, изменение в количество пир поинтов

CREATE OR REPLACE PROCEDURE peerpoint_balance_from_transfer(result INOUT REFCURSOR)
AS
$$
BEGIN
    OPEN result FOR
        SELECT a.peer AS peer, coalesce(a.count, 0) - coalesce(b.count, 0) AS points_change
        FROM (SELECT peer1 AS peer, count(*) FROM transferred_points_human_readable() GROUP BY peer1) AS a
                 FULL JOIN
             (SELECT peer2 AS peer, count(*) FROM transferred_points_human_readable() GROUP BY peer2) AS b
                 ON a.peer = b.peer
        ORDER BY 2 DESC, 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL peerpoint_balance_from_transfer('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 6) Определить самое часто проверяемое задание за каждый день
-- При одинаковом количестве проверок каких-то заданий в определенный день, вывести их все. \
-- Формат вывода: день, название задания

CREATE OR REPLACE PROCEDURE frequently_checked_task(result INOUT REFCURSOR)
AS
$$
BEGIN
    OPEN result FOR
        SELECT a.date AS day, a.task FROM
        (SELECT task,
                rank() OVER (PARTITION BY date ORDER BY count(*) DESC),
                count(*),
                date
         FROM checks
         GROUP BY task, date
        ) AS a
        WHERE rank = 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL frequently_checked_task('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
-- Параметры процедуры: название блока, например "CPP". \
-- Результат вывести отсортированным по дате завершения. \
-- Формат вывода: ник пира, дата завершения блока (т.е. последнего выполненного задания из этого блока)

CREATE OR REPLACE PROCEDURE peer_end_block(name TEXT, result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        SELECT checks.peer, checks.date AS day
        FROM checks
        WHERE checks.task = 'project_' || name || '4'
        ORDER BY 2 DESC, 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL peer_end_block('2','my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 8) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира, проверяться у которого рекомендует наибольшее число друзей. \
-- Формат вывода: ник пира, ник найденного проверяющего

CREATE OR REPLACE PROCEDURE peer_for_check(result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        SELECT name as peer, recommended_peer FROM
        (SELECT name,
                row_number() OVER (PARTITION BY name ORDER BY count(*) DESC),
                count(*),
                recommended_peer
         FROM peers
            INNER JOIN friends ON name = peer1
            INNER JOIN recommendations ON peer2 = peer
         GROUP BY name, recommended_peer
         ORDER BY 1, 3 DESC
        ) AS t
        where row_number = 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL peer_for_check('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 9) Определить процент пиров, которые:
-- - Приступили только к блоку 1
-- - Приступили только к блоку 2
-- - Приступили к обоим
-- - Не приступили ни к одному
--
-- Пир считается приступившим к блоку, если он проходил хоть одну проверку любого задания из этого блока (по таблице Checks)
--
-- Параметры процедуры: название блока 1, например SQL, название блока 2, например A. \
-- Формат вывода: процент приступивших только к первому блоку, процент приступивших только ко второму блоку, процент приступивших к обоим, процент не приступивших ни к одному

CREATE OR REPLACE PROCEDURE start_blocks(block1 TEXT, block2 TEXT, result INOUT REFCURSOR) AS
$$
DECLARE
    count_peers NUMERIC := (SELECT count(*)
                            FROM peers);
    first       NUMERIC := (SELECT count(*)
                            FROM peers
                            WHERE exists(SELECT 1
                                         FROM checks
                                         WHERE checks.peer = peers.name AND
                                               left(checks.task, length(block1)) = block1)
                           ) / count_peers;
    second      NUMERIC := (SELECT count(*)
                            FROM peers
                            WHERE
exists(SELECT 1
       FROM checks
       WHERE checks.peer = peers.name AND
             left(checks.task, length(block2)) = block2)) / count_peers;
BEGIN
    OPEN result FOR
        SELECT round(first * (1 - second) * 100)       AS Started_Block1,
               round(second * (1 - first) * 100)       AS Started_Block2,
               round(first * second * 100)             AS Started_Both_Blocks,
               round((1 - first) * (1 - second) * 100) AS Didnt_Start_Any_Block;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL start_blocks('project_2', 'project_1', 'my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
-- Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения. \
-- Формат вывода: процент пиров, успешно прошедших проверку в день рождения, процент пиров, проваливших проверку в день рождения

CREATE OR REPLACE PROCEDURE birthday_checks(result INOUT REFCURSOR)
AS
$$
DECLARE
    count_peers NUMERIC := (SELECT count(*)
                            FROM (SELECT DISTINCT name
                                  FROM peers
                                     JOIN checks c ON peers.name = c.peer AND
                                     date_part('month', birthday) = date_part('month', date) AND
                                     date_part('day', birthday) = date_part('day', date)
                                     ) AS t);
    all_peers NUMERIC := (SELECT count(*) FROM peers);
BEGIN
    OPEN result FOR
        SELECT round(count_peers/all_peers * 100)       AS Successful_Checks,
               round((1 - count_peers/all_peers) * 100) AS Unsuccessful_Checks;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL birthday_checks('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
-- Параметры процедуры: названия заданий 1, 2 и 3. \
-- Формат вывода: список пиров

CREATE OR REPLACE PROCEDURE peers_123(task1 TEXT, task2 TEXT, task3 TEXT, result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        SELECT peer FROM checks
	        JOIN p2p ON checks.id = p2p."check" AND p2p.state = 'success' AND (checks.task = task1 OR checks.task = task2) AND
	                    ((exists(SELECT 1 FROM mas_tasks WHERE mas_tasks.title = checks.task) AND
	    	            (select count(*) FROM verter WHERE verter.state = 'success' AND verter."check" = checks.id) = 1)
	                    OR NOT exists(SELECT 1 FROM mas_tasks WHERE mas_tasks.title = checks.task))
                INTERSECT
        SELECT peer FROM checks
	        JOIN p2p ON checks.id = p2p."check" AND checks.task = task3 AND (p2p.state = 'failure' OR
	                    (exists(SELECT 1 FROM mas_tasks WHERE mas_tasks.title = checks.task) AND
		                NOT exists(SELECT 1 FROM verter WHERE verter.state = 'success' AND verter."check" = checks.id)));
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL peers_123('project_10', 'project_20', 'project_22', 'my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
-- То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей. \
-- Формат вывода: название задачи, количество предшествующих

CREATE OR REPLACE PROCEDURE count_prev_project(result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        WITH RECURSIVE projects AS (
            SELECT 'project_00' AS task,
                    0 AS Prev_Count
                UNION
            SELECT (SELECT title FROM tasks WHERE parent = task) AS name,
                   Prev_Count + 1 AS Prev_Count
            FROM projects
            WHERE task != 'project_44')
        SELECT * FROM projects;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL count_prev_project('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 13) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы *N* идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок *N*. \
-- Временем проверки считать время начала P2P этапа. \
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных. \
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального. \
-- Формат вывода: список дней

CREATE OR REPLACE PROCEDURE happy_days(par_days INT,result INOUT REFCURSOR) AS
$$
DECLARE
   row RECORD;
   i INTEGER := 0;
   prev_date DATE := (SELECT date FROM checks JOIN p2p ON checks.id = p2p."check"
                      WHERE state <> 'start'
                      ORDER BY date, time LIMIT 1);
BEGIN
   DROP TABLE IF EXISTS temp;
   CREATE TABLE temp(date DATE, time TIME, state INT, sum INT);
   FOR row IN
        (SELECT date, time,
                CASE state
	            WHEN 'success' THEN 1 ELSE 0 END AS c
         FROM checks
            JOIN p2p ON checks.id = p2p."check"
            JOIN xp ON xp."check" = checks.id
            JOIN tasks ON tasks.title = task
         WHERE state <> 'start' AND xp_amount >= 0.8 * max_xp
         ORDER BY date, time)
   LOOP
	  IF row.date <> prev_date THEN
	  i = 0;
	  prev_date = row.date;
	  END IF;
      IF row.c = 0 THEN
         i = 0;
      ELSE
         i = i + 1;
      END IF;
      INSERT INTO temp(date,time,state,sum) VALUES(row.date,row.time,row.c,i);
   END LOOP;
   OPEN result FOR
   SELECT date FROM temp GROUP BY date HAVING max(sum) >= par_days ORDER BY date;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL happy_days(1,'my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 14) Определить пира с наибольшим количеством XP
-- Формат вывода: ник пира, количество XP

CREATE OR REPLACE PROCEDURE max_xp(result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        SELECT c.peer, sum(xp_amount) AS xp FROM xp
            INNER JOIN public.checks c ON c.id = xp."check"
        GROUP BY c.peer
        ORDER BY 2 DESC LIMIT 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL max_xp('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 15) Определить пиров, приходивших раньше заданного времени не менее *N* раз за всё время
-- Параметры процедуры: время, количество раз *N*. \
-- Формат вывода: список пиров

CREATE OR REPLACE PROCEDURE early_coming(par_time TIME,par_count INT,result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        SELECT peer FROM (SELECT peer, date, min(time) FROM time_tracking
                          WHERE state = 1 AND time < par_time
                          GROUP BY date,peer ) AS t
        GROUP BY peer HAVING count(*) >= par_count;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL early_coming('8:00',3,'my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 16) Определить пиров, выходивших за последние *N* дней из кампуса больше *M* раз
-- Параметры процедуры: количество дней *N*, количество раз *M*. \
-- Формат вывода: список пиров

CREATE OR REPLACE PROCEDURE last_day_coming(day_count INT, par_count INT, result INOUT REFCURSOR) AS
$$
BEGIN
    OPEN result FOR
        SELECT peer FROM (SELECT peer, date
                          FROM time_tracking
                          WHERE date > current_date - day_count
                          GROUP BY date,peer
                          ORDER BY 1,2) AS t
        GROUP BY peer HAVING count(*) > par_count;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL last_day_coming(810,3,'my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;

-- ##### 17) Определить для каждого месяца процент ранних входов
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус за всё время (будем называть это общим числом входов). \
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус раньше 12:00 за всё время (будем называть это числом ранних входов). \
-- Для каждого месяца посчитать процент ранних входов в кампус относительно общего числа входов. \
-- Формат вывода: месяц, процент ранних входов

CREATE OR REPLACE FUNCTION procent_early_coming_in_month(current_month timestamp)
    RETURNS TABLE (
                    Month TEXT,
                    EarlyEntries NUMERIC,
                    sum DOUBLE PRECISION
                  )
AS
$$
DECLARE
    count_peers NUMERIC := (SELECT count(name) FROM (
                                SELECT name FROM peers
                                INNER JOIN time_tracking ON name = peer
                                WHERE date_part('month', birthday) = date_part('month', current_month) AND
                                      date_part('month', birthday) = date_part('month', date)) AS t);
    count_early_peers NUMERIC := (SELECT count(name) FROM (
                                      SELECT name FROM peers
                                      INNER JOIN time_tracking ON name = peer
                                      WHERE date_part('month', birthday) = date_part('month', current_month) AND
                                            date_part('month', birthday) = date_part('month', date) AND
                                            time < '12:00') AS t);
BEGIN
    RETURN QUERY
        SELECT to_char(current_month, 'month'),
               round(count_early_peers/(CASE WHEN count_peers != 0 THEN count_peers ELSE 1 END)*100),
               date_part('month', current_month);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE procent_early_coming(result INOUT REFCURSOR) AS
$$
BEGIN
  OPEN result FOR
      SELECT Month, EarlyEntries as Early_Entries  FROM (
        SELECT * FROM procent_early_coming_in_month(timestamp '1999-01-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-02-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-02-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-03-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-04-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-05-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-06-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-07-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-08-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-09-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-10-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-11-01')
          UNION SELECT * FROM procent_early_coming_in_month(timestamp '1999-12-01')
        ORDER BY sum) AS t;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL procent_early_coming('my_cursor');
FETCH ALL IN "my_cursor";
COMMIT;
