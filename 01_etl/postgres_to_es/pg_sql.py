PG_SELECT_BY_ID = """SELECT fw.id,
                            fw.rating AS imdb_rating,
                            array_agg(DISTINCT g.name)  AS genre,
                            fw.title,
                            fw.description,
                            COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director'), '{}')                  AS director,
                      array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor')                                             AS actors_names,
                      array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer')                                            AS writers_names,
                      coalesce(json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE pfw.role = 'actor'),'[]')   AS actors,
                      coalesce(json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE pfw.role = 'writer'),'[]')  AS writers
                 FROM content.film_work fw
                 LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                 LEFT JOIN content.person p ON p.id = pfw.person_id
                 LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                 LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.id in %s
                GROUP BY fw.id
                ORDER BY fw.modified"""

PG_SELECT_ALL = """SELECT fw.id,
                          fw.rating AS imdb_rating,
                          array_agg(DISTINCT g.name)  AS genre,
                          fw.title,
                          fw.description,
                          coalesce(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director'), '{}')                        AS director,
                          array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor')                                             AS actors_names,
                          array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer')                                            AS writers_names,
                          coalesce(json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE pfw.role = 'actor'),'[]')   AS actors,
                          coalesce(json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE pfw.role = 'writer'),'[]')  AS writers
                     FROM content.film_work fw
                     LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                     LEFT JOIN content.person p ON p.id = pfw.person_id
                     LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                     LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    GROUP BY fw.id
                    ORDER BY fw.modified"""

PG_MOVIES_TO_UPDATE = """SELECT m.id,
                                max(m.modified) AS modified
                           FROM (SELECT pfw.film_work_id AS id,
                                        p.modified
                                   FROM content.person p,
                                        content.person_film_work pfw
                                  WHERE p.id = pfw.person_id
                                    AND p.modified > %(date)s
                                  UNION ALL 
                                 SELECT gfw.film_work_id AS id,
                                        g.modified
                                   FROM content.genre g,
                                        content.genre_film_work gfw
                                  WHERE g.id = gfw.genre_id
                                    AND g.modified > %(date)s
                                  UNION ALL
                                 SELECT f.id,
                                        f.modified
                                   FROM content.film_work f
                                  WHERE f.modified > %(date)s) m
                          GROUP BY m.id
                          ORDER BY max(m.modified);"""

PG_LAST_MODIFIED = """SELECT max(m.modified) AS modified
                        FROM (SELECT max(p.modified) AS modified
                                FROM content.person p
                               UNION ALL
                              SELECT max(g.modified) AS modified
                                FROM content.genre g
                               UNION ALL
                              SELECT max(f.modified) AS modified
                                FROM content.film_work f) m;"""
