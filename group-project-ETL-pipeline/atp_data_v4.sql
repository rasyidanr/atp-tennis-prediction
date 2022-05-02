create schema atp_tour_schema;

drop table if exists atp_tour_schema.atp_indianwells_single CASCADE;
drop table if exists atp_tour_schema.atp_indianwells_double CASCADE;
drop table if exists atp_tour_schema.atp_leaderboard CASCADE;
drop table if exists atp_tour_schema.atp_rank CASCADE;
drop table if exists atp_tour_schema.tournament CASCADE;


CREATE TABLE atp_tour_schema."atp_rank" (
  "id_player" int PRIMARY KEY,
  "player" varchar,
  "rank" varchar,
  "age" varchar,
  "player_point" varchar ,
  "tournament_played" varchar ,
  "next_best" varchar
);


CREATE TABLE atp_tour_schema."tournament" (
  "id_tournament" int PRIMARY KEY,
  "tournament_name" varchar,
  "tournament_location" varchar,
  "tournament_date" varchar,
  "tournament_draw" varchar,
  "tournament_surface" varchar,
  "tournament_financial_commitment" varchar
);

CREATE TABLE atp_tour_schema."atp_leaderboard" (
  "id_leaderboard" int PRIMARY KEY,
  "id_player" int null REFERENCES atp_tour_schema.atp_rank("id_player"),
  "player" varchar,
  "serve_rating" float,
  "1st_serve" float,
  "1st_serve_points_won" float,
  "2nd_serve_points_won" float,
  "service_games_won_percent" float,
  "avg_aces_per_match" float,
  "avg_double_faults_per_match" float
);

CREATE TABLE atp_tour_schema."atp_indianwells_single" (
  "id_match" int PRIMARY KEY,
  "id_tournament" int REFERENCES atp_tour_schema.tournament("id_tournament") on delete CASCADE,
  "id_player_1" int null REFERENCES atp_tour_schema.atp_rank("id_player") on delete CASCADE,
  "id_player_2" int null REFERENCES atp_tour_schema.atp_rank("id_player") on delete CASCADE,
  "player_1" varchar,
  "player_2" varchar,
  "score" varchar,
  "set_1" varchar,
  "set_2" varchar,
  "set_3" varchar
);

CREATE TABLE atp_tour_schema."atp_indianwells_double" (
  "id_match" int PRIMARY KEY,
  "id_tournament" int REFERENCES atp_tour_schema.tournament("id_tournament") on delete CASCADE,
  "id_player_t1_1" int null REFERENCES atp_tour_schema.atp_rank("id_player") on delete CASCADE,
  "id_player_t1_2" int null REFERENCES atp_tour_schema.atp_rank("id_player") on delete CASCADE,
  "id_player_t2_1" int null REFERENCES atp_tour_schema.atp_rank("id_player") on delete CASCADE,
  "id_player_t2_2" int null REFERENCES atp_tour_schema.atp_rank("id_player") on delete CASCADE,
  "t1_player_1" varchar,
  "t1_player_2" varchar,
  "t2_player_1" varchar,
  "t2_player_2" varchar,
  "score" varchar,
  "set_1" varchar,
  "set_2" varchar,
  "set_3" varchar
);
