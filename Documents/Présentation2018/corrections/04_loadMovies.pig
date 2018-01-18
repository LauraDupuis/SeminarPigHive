RATING_A = LOAD 'data/ratings.csv' USING PigStorage(',') as (user_id: long,movie_id: long,rating: double,timestamp: long);
RATING_B = FILTER RATING_A BY user_id IS NOT NULL;
RATING_C = FOREACH RATING_B GENERATE user_id,movie_id,rating,ToDate(timestamp) as ts;
STORE RATING_C INTO 'movie.ratings' USING org.apache.hive.hcatalog.pig.HCatStorer();


TAG_A = LOAD 'data/tags.csv' USING PigStorage(',') as (user_id: long,movie_id: long,tag: chararray,timestamp: long);
TAG_B = FILTER TAG_A BY user_id IS NOT NULL;
TAG_C = FOREACH TAG_B GENERATE user_id,movie_id,tag,ToDate(timestamp) as ts;
STORE TAG_C INTO 'movie.tags' USING org.apache.hive.hcatalog.pig.HCatStorer();


MOVIE_A = LOAD 'data/movies.csv' USING PigStorage(',') as (movie_id: long,title: chararray,genres: chararray);
MOVIE_B = FILTER MOVIE_A BY movie_id IS NOT NULL;
MOVIE_C = FOREACH MOVIE_B GENERATE movie_id, title, STRSPLITTOBAG(genres, '\\|', 0) as genres:{(chararray)};
STORE MOVIE_C INTO 'movie.movies' USING org.apache.hive.hcatalog.pig.HCatStorer();


LINKS_A = LOAD 'data/links.csv' USING PigStorage(',') as (movie_id: long,imdbid: chararray,tmdbid: chararray);
LINKS_B = FILTER LINKS_A BY movie_id IS NOT NULL;
LINKS_C = FOREACH LINKS_B GENERATE movie_id,
        CONCAT('https://movielens.org/movies/', (chararray)movie_id) as url_ml,
        CONCAT('http://www.imdb.com/title/tt',imdbid,'/') as url_im,
        CONCAT('https://www.themoviedb.org/movie/', tmdbid) as url_tm;
STORE LINKS_C INTO 'movie.links' USING org.apache.hive.hcatalog.pig.HCatStorer();



A = LOAD 'movie.links' USING org.apache.hive.hcatalog.pig.HCatLoader();
B = LIMIT A 10;
