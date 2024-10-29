DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE peers
(
    name     TEXT PRIMARY KEY,
    birthday DATE
);

CREATE TABLE tasks
(
    title  TEXT PRIMARY KEY,
    parent TEXT,
    max_xp INT,
    FOREIGN KEY (parent) REFERENCES tasks
);

CREATE TABLE checks
(
    id   SERIAL PRIMARY KEY,
    peer TEXT,
    task TEXT,
    date DATE,
    FOREIGN KEY (peer) REFERENCES peers,
    FOREIGN KEY (task) REFERENCES tasks
);

CREATE TYPE check_state AS ENUM ('start', 'success', 'failure');

CREATE TABLE p2p
(
    id            SERIAL PRIMARY KEY,
    "check"        INT,
    checking_peer TEXT,
    state         check_state,
    time          TIME,
    FOREIGN KEY ("check") REFERENCES checks,
    FOREIGN KEY (checking_peer) REFERENCES peers
);

CREATE OR REPLACE FUNCTION trigger_function_p2p() RETURNS TRIGGER AS
$$
BEGIN
    IF ((new.state != 'start' AND
         exists(SELECT * FROM p2p WHERE state = 'start' AND p2p."check" = new."check")) OR
        (new.state = 'start' AND
         NOT exists (SELECT * FROM p2p WHERE p2p."check" = new."check"))
       ) THEN
        RETURN new;
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_insert_p2p
    BEFORE INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function_p2p();

CREATE TABLE verter
(
    id     SERIAL PRIMARY KEY,
    "check" INT,
    state  check_state,
    time   TIME,
    FOREIGN KEY ("check") REFERENCES checks
);

CREATE OR REPLACE FUNCTION trigger_function_verter() RETURNS TRIGGER AS
$$
BEGIN
    IF ((SELECT count(*) FROM p2p WHERE new."check" = p2p."check" AND state = 'success') = 1 AND
        ((new.state != 'start' AND
          (SELECT count(*) FROM verter WHERE state = 'start' AND "check" = new."check") = 1 AND
          (SELECT count(*) FROM verter WHERE "check" = new."check") = 1) OR
         (new.state = 'start' AND
          NOT exists (SELECT * FROM verter WHERE "check" = new."check")))
        ) THEN
        RETURN new;
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_insert_verter
    BEFORE INSERT
    ON verter
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function_verter();

CREATE TABLE transferred_points
(
    id            SERIAL PRIMARY KEY,
    checking_peer TEXT,
    checked_peer  TEXT,
    points_amount INT,
    FOREIGN KEY (checking_peer) REFERENCES peers,
    FOREIGN KEY (checked_peer) REFERENCES peers,
    CONSTRAINT constraint_unique_transferred_points UNIQUE (checking_peer, checked_peer)
);

CREATE TABLE friends
(
    id    SERIAL PRIMARY KEY,
    peer1 TEXT,
    peer2 TEXT,
    FOREIGN KEY (peer1) REFERENCES peers,
    FOREIGN KEY (peer2) REFERENCES peers,
    CONSTRAINT constraint_unique_friends UNIQUE (peer1, peer2),
    CONSTRAINT constraint_ego_friends CHECK (peer1 != peer2)
);

CREATE OR REPLACE FUNCTION trigger_function_friends() RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO friends(peer1, peer2) VALUES (new.peer2, new.peer1);
    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_insert_friends
    AFTER INSERT
    ON friends
    FOR EACH ROW
    WHEN (pg_trigger_depth() = 0)
EXECUTE PROCEDURE trigger_function_friends();

CREATE TABLE recommendations
(
    id               SERIAL PRIMARY KEY,
    peer             TEXT,
    recommended_peer TEXT,
    FOREIGN KEY (peer) REFERENCES peers,
    FOREIGN KEY (recommended_peer) REFERENCES peers,
    CONSTRAINT constraint_unique_recommendations UNIQUE (peer, recommended_peer),
    CONSTRAINT constraint_ego_recomendations CHECK (peer != recommended_peer)
);

CREATE TABLE xp
(
    id        SERIAL PRIMARY KEY,
    "check"    INT,
    xp_amount INT,
    FOREIGN KEY ("check") REFERENCES checks
);


CREATE TABLE time_tracking
(
    id    SERIAL PRIMARY KEY,
    peer  TEXT,
    date  DATE,
    time  TIME,
    state INT,
    FOREIGN KEY (peer) REFERENCES peers,
    CONSTRAINT constraint_state_time_tracking CHECK (state BETWEEN 1 AND 2)
);

CREATE OR REPLACE PROCEDURE insert_peers(name TEXT) AS
$$
DECLARE
    start_date  DATE    := '1988-01-01';
    random_days INTEGER := random() * 15 * 365;
BEGIN
    INSERT INTO peers VALUES (name, start_date + random_days);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE insert_tasks(amount INT) AS
$$
DECLARE
    prev TEXT := NULL;
    this TEXT := NULL;
BEGIN
    this = 'project_00';
    INSERT INTO tasks VALUES (this, prev, 100);
    prev = this;
    FOR i IN 1 .. amount - 1
        LOOP
            this = 'project_' || (i / 5)::INT || i % 5;
            INSERT INTO tasks VALUES (this, prev, 100 + (random() * 19)::INT * 100);
            prev = this;
        END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE insert_friends_and_recommendations() AS
$$
DECLARE
    maxa INT := (SELECT count(*) FROM peers);
BEGIN
    FOR i IN 1 .. maxa
        LOOP
            BEGIN
                INSERT INTO friends(peer1, peer2)
                VALUES ( (SELECT name FROM peers ORDER BY random() LIMIT 1)
                       , (SELECT name FROM peers ORDER BY random() LIMIT 1));
                INSERT INTO recommendations(peer, recommended_peer)
                VALUES ( (SELECT name FROM peers ORDER BY random() LIMIT 1)
                       , (SELECT name FROM peers ORDER BY random() LIMIT 1));
                EXCEPTION
                WHEN OTHERS THEN
                    maxa := maxa + 1;
            END;
        END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE insert_time_tracking() AS
$$
DECLARE
    maxa        INT  := (SELECT count(*)
                         FROM peers) * 10;
    start_date  DATE := '2021-07-21';
    random_days INTEGER ;
    timee       TIME;
    peer        TEXT;
BEGIN
    FOR i IN 1 .. maxa
        LOOP
            random_days := random() * 2 * 5;
            timee := (SELECT (random() * (INTERVAL '1 day' - INTERVAL '35 min'))::INTERVAL);
            peer := (SELECT name FROM peers ORDER BY random() LIMIT 1);
            INSERT INTO time_tracking(peer, date, time, state)
            VALUES (peer, start_date + random_days, timee, 1);
            INSERT INTO time_tracking(peer, date, time, state)
            VALUES (peer, start_date + random_days, timee + INTERVAL '35 min'::INTERVAL, 2);
        END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE insert_p2p(peer_name TEXT, reviewer TEXT, task_name TEXT, state check_state, timee TIME, dat date) AS
$$
BEGIN
    IF (state = 'start') THEN
        INSERT INTO checks(peer, task, date) VALUES (peer_name, task_name, dat);
        INSERT INTO p2p("check", checking_peer, state, time)
        VALUES ((SELECT max(id) FROM checks), reviewer, state, timee);
    ELSE
        INSERT INTO p2p("check", checking_peer, state, time)
        VALUES ((SELECT id
                 FROM checks
                 WHERE peer = peer_name AND
                       task = task_name AND
                       date = dat
                 ORDER BY id DESC LIMIT 1),
                reviewer, state, timee);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE insert_p2p_rec(par_task TEXT, par_peer TEXT, koef NUMERIC) AS
$$
DECLARE
    par_parent   TEXT        := (SELECT parent
                                 FROM tasks
                                 WHERE title = par_task);
    timee        TIME        := (SELECT (random() * (INTERVAL '1 day' - INTERVAL '35 min'))::INTERVAL);
    statee       check_state := 'failure';
    par_reviewer TEXT         = (SELECT name
                                 FROM peers
                                 WHERE name != par_peer
                                 ORDER BY random()
                                 LIMIT 1);
    start_date   DATE        := '2022-07-01';
	dat          DATE        := start_date + (koef * to_number(right(par_task, 2),'99'))::INT;
BEGIN
    IF ((SELECT parent FROM tasks WHERE title = par_task) IS NOT NULL AND
        NOT exists(SELECT * FROM checks WHERE task = par_parent AND peer = par_peer))
    THEN
        CALL insert_p2p_rec(par_parent, par_peer,koef);
    END IF;
    CALL insert_p2p(par_peer, par_reviewer, par_task, 'start',
                    timee,dat);
    IF (random() > 0.2) THEN
        statee = 'success';
    END IF;
    CALL insert_p2p(par_peer, par_reviewer, par_task, statee,
                    timee + (random() * INTERVAL '30 min')::INTERVAL, dat);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE insert_p2p(amount INT) AS
$$
DECLARE
    project TEXT := NULL;
	row RECORD;
	koef NUMERIC;
BEGIN
    FOR row IN
	(SELECT * FROM peers)
        LOOP
		    koef = random() * 13;
            project = (SELECT title FROM tasks ORDER BY random() LIMIT 1);
            CALL insert_p2p_rec(project, row.name,koef);
        END LOOP;
END
$$ LANGUAGE plpgsql;

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

CREATE OR REPLACE PROCEDURE insert_verter() AS
$$
DECLARE
    maxa  INT := (SELECT count(*) FROM tasks) / 3;
    stat  check_state;
    tasks TEXT[];
    row   RECORD;
BEGIN
    SELECT array_agg(title) FROM mas_tasks INTO tasks;
    FOR row IN (SELECT * FROM p2p
                    JOIN checks ON checks.id = "check" AND state = 'success' AND task = ANY (tasks))
        LOOP
            INSERT INTO verter("check", state, time)
            VALUES (row."check", 'start', row.time + (random() * INTERVAL '1 min')::INTERVAL);
            IF (random() > 0.2) THEN
                stat = 'success';
            ELSE
                stat = 'failure';
            END IF;
            INSERT INTO verter("check", state, time)
            VALUES (row."check", stat, row.time + (random() * INTERVAL '2 min' + INTERVAL '1 min')::INTERVAL);
        END LOOP;
END
$$ LANGUAGE plpgsql;

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

CREATE OR REPLACE FUNCTION trigger_function_insert_xp_p2p() RETURNS TRIGGER AS
$$
DECLARE
    maxa INT := (SELECT max_xp FROM tasks
                     JOIN checks ON checks.task = tasks.title
                 WHERE checks.id = new."check");
BEGIN
    IF (new.state = 'success' AND
        NOT exists(SELECT * FROM checks
                       JOIN mas_tasks ON mas_tasks.title = checks.task AND new."check" = checks.id))
    THEN
        INSERT INTO xp("check", xp_amount) VALUES (new."check", (random() + 1) * maxa / 2);
    END IF;
    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER xp_insert_trigger_p2p
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function_insert_xp_p2p();

CREATE OR REPLACE FUNCTION trigger_function_insert_xp_verter() RETURNS TRIGGER AS
$$
DECLARE
    maxa INT := (SELECT max_xp FROM tasks
                     JOIN checks ON checks.task = tasks.title
                 WHERE checks.id = new."check");
BEGIN
    IF (new.state = 'success' AND
        exists(SELECT * FROM checks
                   JOIN mas_tasks ON mas_tasks.title = checks.task AND new."check" = checks.id))
    THEN
        INSERT INTO xp("check", xp_amount) VALUES (new."check", (random() + 1) * maxa / 2);
    END IF;
    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER xp_insert_trigger_verter
    AFTER INSERT
    ON verter
    FOR EACH ROW
EXECUTE PROCEDURE trigger_function_insert_xp_verter();


CALL insert_peers('gemmaeme');
CALL insert_peers('demogorg');
CALL insert_peers('kerenhor');
CALL insert_peers('kenneyla');
CALL insert_peers('sensordo');
CALL insert_peers('zoraidab');
CALL insert_peers('julietah');
CALL insert_peers('coriande');
CALL insert_peers('sommerha');
CALL insert_peers('littleca');
CALL insert_peers('waltlate');
CALL insert_peers('synthiat');
CALL insert_peers('norridge');
CALL insert_peers('cathleeb');
CALL insert_peers('toppingk');
CALL insert_peers('nylabeck');
CALL insert_peers('duffmank');
CALL insert_peers('sharkmer');
CALL insert_peers('julissam');
CALL insert_peers('kaleighh');
CALL insert_peers('erikurfo');
CALL insert_peers('hanalesh');
CALL insert_peers('sevenstr');
CALL insert_peers('berylkos');
CALL insert_peers('carisafi');
CALL insert_peers('ellyntom');
CALL insert_peers('archimed');
CALL insert_peers('varlybot');
CALL insert_peers('meghanki');
CALL insert_peers('ebonicra');
CALL insert_peers('harlenev');
CALL insert_peers('cleotilm');
CALL insert_peers('mauricio');
CALL insert_peers('desirelo');
CALL insert_peers('michaele');
CALL insert_peers('endadeir');
CALL insert_peers('adinamar');
CALL insert_peers('toffeeco');
CALL insert_peers('randyrau');
CALL insert_peers('flatulek');
CALL insert_peers('nerissas');
CALL insert_peers('rosieamb');
CALL insert_peers('azathoth');
CALL insert_peers('mavissig');
CALL insert_peers('hammondy');
CALL insert_peers('tressiel');
CALL insert_peers('erikursi');
CALL insert_peers('grapefru');
CALL insert_peers('jenegabr');
CALL insert_peers('shockere');
CALL insert_peers('chiquita');
CALL insert_peers('errokele');
CALL insert_peers('oderover');
CALL insert_peers('aethando');
CALL insert_peers('willylyd');
CALL insert_peers('celestac');
CALL insert_peers('luanarau');
CALL insert_peers('eusebiaa');
CALL insert_peers('gehnbrig');
CALL insert_peers('directrh');
CALL insert_peers('umaradri');
CALL insert_peers('abbeyrus');
CALL insert_peers('bagshotw');
CALL insert_peers('pottluci');
CALL insert_peers('bulstrod');
CALL insert_peers('glutenlu');
CALL insert_peers('yesenias');
CALL insert_peers('shenmurr');
CALL insert_peers('freejerm');
CALL insert_peers('verdaqui');
CALL insert_peers('godardje');
CALL insert_peers('peachgha');
CALL insert_peers('skytekel');
CALL insert_peers('kaylebee');
CALL insert_peers('emilielu');
CALL insert_peers('necromat');
CALL insert_peers('ibbenber');
CALL insert_peers('shaunnaa');
CALL insert_peers('cadwynva');
CALL insert_peers('professo');
CALL insert_peers('makedaho');
CALL insert_peers('botleyla');
CALL insert_peers('scrimgeo');
CALL insert_peers('umfredir');
CALL insert_peers('lilliefe');
CALL insert_peers('hoochsha');
CALL insert_peers('berriesi');
CALL insert_peers('hazzeapi');
CALL insert_peers('nicolsha');
CALL insert_peers('audiecle');
CALL insert_peers('perrosde');
CALL insert_peers('gerardba');
CALL insert_peers('hayheadk');
CALL insert_peers('ammoshri');
CALL insert_peers('ushamyle');
CALL insert_peers('eleonorf');
CALL insert_peers('ngocgrag');
CALL insert_peers('victario');
CALL insert_peers('schrader');
CALL insert_peers('scottcen');
CALL insert_peers('wongburg');
CALL insert_peers('bernadin');
CALL insert_peers('autumnga');
CALL insert_peers('georgett');
CALL insert_peers('golemtam');
CALL insert_peers('lightang');
CALL insert_peers('tracielo');
CALL insert_peers('sanddony');
CALL insert_peers('kenchjen');
CALL insert_peers('vulpixta');
CALL insert_peers('reckonwi');
CALL insert_peers('wallacei');
CALL insert_peers('addaclic');
CALL insert_peers('leilaniy');
CALL insert_peers('mlkshkmo');
CALL insert_peers('winkyexi');
CALL insert_peers('latoyiac');
CALL insert_peers('lavelley');
CALL insert_peers('kentonch');
CALL insert_peers('thomasik');
CALL insert_peers('griffinp');
CALL insert_peers('roscoesu');
CALL insert_peers('seftonca');
CALL insert_peers('hildabur');
CALL insert_peers('dotharer');
CALL insert_peers('cranberr');
CALL insert_peers('puckdudl');
CALL insert_peers('vileplme');
CALL insert_peers('banefort');
CALL insert_peers('lakiesha');
CALL insert_peers('cynricge');
CALL insert_peers('lessiety');
CALL insert_peers('hatchesn');
CALL insert_peers('stepanie');
CALL insert_peers('cornmeal');
CALL insert_peers('curtnorj');
CALL insert_peers('slyviada');
CALL insert_peers('roderick');
CALL insert_peers('sleepyka');
CALL insert_peers('kavulada');
CALL insert_peers('dondarri');
CALL insert_peers('pipebomb');
CALL insert_peers('accordij');
CALL insert_peers('floretta');
CALL insert_peers('otheymal');
CALL insert_peers('octaviar');
CALL insert_peers('moqorroj');
CALL insert_peers('harodonf');
CALL insert_peers('eddacris');
CALL insert_peers('bonnyped');
CALL insert_peers('alidaefo');
CALL insert_peers('visenyac');
CALL insert_peers('practisd');
CALL insert_peers('berniece');
CALL insert_peers('alphonsk');
CALL insert_peers('roperkat');
CALL insert_peers('adrienne');
CALL insert_peers('kathyhan');
CALL insert_peers('briejone');
CALL insert_peers('breadmit');
CALL insert_peers('katherng');
CALL insert_peers('tulasomm');
CALL insert_peers('jodilael');
CALL insert_peers('rebeccaa');
CALL insert_peers('stygians');
CALL insert_peers('fajitaau');
CALL insert_peers('lasandra');
CALL insert_peers('durranha');
CALL insert_peers('hallietw');
CALL insert_peers('ivelissl');
CALL insert_peers('audiedet');
CALL insert_peers('gaynelle');
CALL insert_peers('qarltonh');
CALL insert_peers('mathosho');
CALL insert_peers('streetms');
CALL insert_peers('revolios');
CALL insert_peers('karryram');
CALL insert_peers('ndeeann');
CALL insert_peers('carriepr');
CALL insert_peers('leilanil');
CALL insert_peers('blooddan');
CALL insert_peers('lajuanak');
CALL insert_peers('clintvic');
CALL insert_peers('snakejac');
CALL insert_peers('stefanie');


CALL insert_tasks(25);

SELECT title
INTO mas_tasks
FROM (SELECT title FROM tasks ORDER BY random() LIMIT (SELECT count(*) FROM tasks) / 3) AS subquery;

CALL insert_p2p(300);
CALL insert_friends_and_recommendations();
CALL insert_verter();
CALL insert_time_tracking();

CREATE OR REPLACE PROCEDURE import_data(file_path TEXT, table_name TEXT) AS
$$
BEGIN
    EXECUTE format('COPY %s FROM %L WITH DELIMITER E''\t''',
                   table_name, file_path);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_data(file_path TEXT, table_name TEXT) AS
$$
BEGIN
    EXECUTE format('COPY %s to %L WITH DELIMITER E''\t''', table_name, file_path);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_all() AS
$$
DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN
        SELECT information_schema.tables.table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        LOOP
            CALL export_data('/Users/waltlate/SQL2_Info21_v1.0-1/datasets/' ||
                             table_name || '.csv', table_name);
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL export_all();
