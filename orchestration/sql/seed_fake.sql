-- Donnees FICTIVES pour construire le dashboard sans attendre le Membre 4.
-- A supprimer / remplacer quand les vraies donnees Gold arrivent. Rejouable.
TRUNCATE stations, gold_mesures_horaires, gold_previsions, gold_capteurs_statut;

INSERT INTO stations VALUES
 ('ST01','Agadir',     30.4278,  -9.5981),
 ('ST02','Casablanca', 33.5731,  -7.5898),
 ('ST03','Rabat',      34.0209,  -6.8416),
 ('ST04','Marrakech',  31.6295,  -7.9811),
 ('ST05','Fes',        34.0331,  -5.0003),
 ('ST06','Tanger',     35.7595,  -5.8340);

-- 7 jours de mesures horaires
INSERT INTO gold_mesures_horaires
SELECT s.station_id, t,
       18 + (abs(hashtext(s.station_id)) % 6)
          + 6 * sin(2*pi()*(extract(hour from t)::float8 - 9)/24) + random(),
       60 + 15 * cos(2*pi()*extract(hour from t)::float8/24) + 3*random(),
       1013 + 4*sin(extract(epoch from t)/86400.0) + random()
FROM stations s,
     generate_series(date_trunc('hour', now()) - interval '7 days',
                     date_trunc('hour', now()), interval '1 hour') AS t;

-- Previsions : 24 h passees (avec reel) + 24 h futures (sans reel)
INSERT INTO gold_previsions
SELECT s.station_id, t,
       18 + (abs(hashtext(s.station_id)) % 6)
          + 6 * sin(2*pi()*(extract(hour from t)::float8 - 9)/24) + 0.5,
       CASE WHEN t <= now()
            THEN 18 + (abs(hashtext(s.station_id)) % 6)
               + 6 * sin(2*pi()*(extract(hour from t)::float8 - 9)/24) + random()
       END
FROM stations s,
     generate_series(date_trunc('hour', now()) - interval '24 hours',
                     date_trunc('hour', now()) + interval '24 hours',
                     interval '1 hour') AS t;

INSERT INTO gold_capteurs_statut VALUES
 ('ST01','OK',        NULL,                  now()),
 ('ST02','OK',        NULL,                  now()),
 ('ST03','DEFAILLANT','valeur bloquee',      now() - interval '3 hours'),
 ('ST04','OK',        NULL,                  now()),
 ('ST05','DEFAILLANT','valeurs hors plage',  now() - interval '1 hour'),
 ('ST06','OK',        NULL,                  now());