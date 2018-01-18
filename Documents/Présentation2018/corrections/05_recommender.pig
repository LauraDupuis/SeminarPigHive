A10 = LOAD 'movie.movies' USING org.apache.hive.hcatalog.pig.HCatLoader();
A1 = LIMIT A10 100;
B1 = FOREACH A1 GENERATE movie_id, FLATTEN(genres) as genre, SIZE(genres) as n;

A20 = LOAD 'movie.movies' USING org.apache.hive.hcatalog.pig.HCatLoader();
A2 = LIMIT A20 100;
B2 = FOREACH A2 GENERATE movie_id, FLATTEN(genres) as genre, SIZE(genres) as n;

C = CROSS B1,B2;
D = FILTER C BY B1::movie_id != B2::movie_id;
E = FOREACH D GENERATE B1::movie_id as m1, B2::movie_id as m2,
            (EqualsIgnoreCase(B1::genre, B2::genre) ? (int)1 : (int)0) as is_same_genre,
            B1::n as n1, B2::n as n2;
F = GROUP E BY (m1, m2);
G = FOREACH F GENERATE group.m1 as m1, group.m2 as m2,
          SUM(E.is_same_genre)/(MIN(E.n1)>MIN(E.n2) ? MIN(E.n2) : MIN(E.n1)) as score;
STORE G INTO 'data/itemsScores3.csv' USING PigStorage('\u001F');
