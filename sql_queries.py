sql_create_schema = """CREATE SCHEMA IF NOT EXISTS mylo;"""

sql_create_players_week = """CREATE TABLE IF NOT EXISTS 
    mylo.player_week_ft (element_ integer, fixture integer, total_points integer, opp_team integer,
    was_home boolean, team_h_score integer, team_a_score integer, round_gw integer, minutes integer,
    goals_scored integer, assists integer, clean_sheets integer, goals_conceded integer, own_goals integer,
    penalties_saved integer, penalties_missed integer, yellow_card integer, red_card integer, save integer,
    bonus integer, bps integer, influence varchar(10), creativity varchar(10), threat varchar(10), ict_index varchar(10),
    value_ply decimal(18,2), 
    CONSTRAINT pk_player_week_id primary key (element_, round_gw),
    CONSTRAINT fk_player_data
      FOREIGN KEY(element_) 
        REFERENCES mylo.player_dm(id))"""

sql_create_players_data = """CREATE TABLE IF NOT EXISTS 
    mylo.player_dm (id integer, name varchar(30), surname varchar(30), form decimal(18,2), 
    total_points integer, now_costs decimal(18,2), team_id integer, position varchar(5),
    CONSTRAINT pk_player_id primary key (id),
    CONSTRAINT fk_team_id
      FOREIGN KEY(team_id) 
        REFERENCES mylo.team_dm(id))"""

sql_create_teams_data = """CREATE TABLE IF NOT EXISTS 
        mylo.team_dm (id integer, name varchar(30), short_name varchar(10), strength_att_home integer, 
        strength_def_home integer, strength_att_away integer, strength_def_away integer, code integer,
        CONSTRAINT pk_team_id primary key (id))"""

sql_insert_ply_gen_postgree = """INSERT INTO mylo.player_dm (id, name, surname, form, total_points, now_costs, team_id, position) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET 
                        name = %s,
                        surname = %s,
                        form = %s,
                        total_points = %s,
                        now_costs = %s,
                        team_id = %s,
                        position = %s"""

sql_insert_weeks_postgree = """INSERT INTO mylo.player_week_ft
                            (element_,
                            fixture,
                            total_points,
                            opp_team,
                            was_home,
                            team_h_score,
                            team_a_score,
                            round_gw,
                            minutes,
                            goals_scored,
                            assists,
                            clean_sheets,
                            goals_conceded,
                            own_goals,
                            penalties_saved,
                            penalties_missed,
                            yellow_card,
                            red_card,
                            save,
                            bonus,
                            bps,
                            influence,
                            creativity,
                            threat,
                            ict_index,
                            value_ply)
                            VALUES (%s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, 
                                    %s)
                                    ON CONFLICT (element_, round_gw) DO UPDATE SET 
                                    fixture = %s,
                                    total_points = %s,
                                    opp_team = %s,
                                    was_home = %s,
                                    team_h_score = %s,
                                    team_a_score = %s,
                                    minutes = %s,
                                    goals_scored = %s,
                                    assists = %s,
                                    clean_sheets = %s,
                                    goals_conceded = %s,
                                    own_goals = %s,
                                    penalties_saved = %s,
                                    penalties_missed = %s,
                                    yellow_card = %s,
                                    red_card = %s,
                                    save = %s,
                                    bonus = %s,
                                    bps = %s,
                                    influence = %s,
                                    creativity = %s,
                                    threat = %s,
                                    ict_index = %s,
                                    value_ply = %s
                                    """

sql_insert_teams_postgree = """INSERT INTO mylo.team_dm
                (id, name, short_name, strength_att_home, strength_def_home, strength_att_away, strength_def_away, code)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET 
                                    name = %s,
                                    short_name = %s,
                                    strength_att_home = %s,
                                    strength_def_home = %s,
                                    strength_att_away = %s,
                                    strength_def_away = %s,
                                    code = %s
                """

basic_ply_data = """select name, surname, form from mylo.player_dm pg order by form desc limit 9"""

value_per_points = """select name, surname, total_points
                        , cast(total_points/now_costs*10 as decimal(12,2)) as point_value, now_costs 
                        from mylo.player_dm pg order by total_points desc limit 9
                        """

detailed_ply_data_per_week = '''
    with ct_gen as (
        select 
        pg.id
        ,pg.name || ' ' || pg.surname as ply_name
        ,"position" 
        ,tg."short_name" as team_name
        from mylo.player_dm pg 
        left join mylo.team_dm tg 
        on tg.id = pg.team_id 
        ),
        ct_stats_totals as (
        select 
        element_
        ,sum(total_points) as total_points 
        ,sum(bps) as bonus_points
        from mylo.player_week_ft pw 
        group by 
        element_
        ),
        last_3_wk_numb as (
        select distinct
        round_gw
        from mylo.player_week_ft pw 
        order by round_gw desc limit 3
        ),
        ct_last_weeks_3 as (
        select 
        element_
        , round_gw
        , total_points 
        from mylo.player_week_ft pw 
        where round_gw in (select * from last_3_wk_numb)
        ),
        ct_last_weeks_3_final as (
        select element_
            ,Max(total_points) filter (where round_gw = (select round_gw from (select round_gw, row_number() over (order by round_gw desc) as rn from last_3_wk_numb) b where rn=1)) as "gw-1"
            ,Max(total_points) filter (where round_gw = (select round_gw from (select round_gw, row_number() over (order by round_gw desc) as rn from last_3_wk_numb) b where rn=2)) as "gw-2"
            ,Max(total_points) filter (where round_gw = (select round_gw from (select round_gw, row_number() over (order by round_gw desc) as rn from last_3_wk_numb) b where rn=3)) as "gw-3"
        from ct_last_weeks_3
        group by element_
        ),
        ct_avg_points_last_4 as (
        select 
        element_ 
        ,round(sum(total_points::decimal)/4,2) as avg_4_weeks
        from mylo.player_week_ft pw 
        where round_gw in (select distinct round_gw from mylo.player_week_ft order by round_gw desc limit 4)
        group by element_
        )
        
        select 
        id
        , ply_name
        , position
        , team_name
        , coalesce(total_points, 0) as total_points
        , bonus_points
        , avg_4_weeks
        , "gw-1" as gwminus1
        , "gw-2" as gwminus2
        , "gw-3" as gwminus3
        from ct_gen
        left join ct_stats_totals
        on ct_stats_totals.element_ = ct_gen.id
        left join ct_last_weeks_3_final
        on ct_last_weeks_3_final.element_ = ct_gen.id
        left join ct_avg_points_last_4 
        on ct_avg_points_last_4.element_ = ct_gen.id
        order by total_points desc limit 15;
    '''

sql_get_salah_id = """
                SELECT id from mylo.player_dm where surname = 'Salah' 
                """

sql_control_of_joins = """
                        SELECT count(id) from mylo.player_week_ft pw  
                        left join mylo.player_dm pd 
                        ON pd.id = pw.element_ where round_gw is null 
                        """

sql_create_players_x_stats = """
                        CREATE TABLE IF NOT EXISTS 
                        mylo.player_stats_dm (id integer, full_name varchar(60), min_played integer, key_passes integer,
                        assists integer, shots integer, xg decimal(18,2), xa decimal(18,2), match_id integer,
                        CONSTRAINT pk_player_stats_id primary key (id, match_id)) 
                        """

sql_insert_players_x_stats = """INSERT INTO mylo.player_stats_dm
                (id, full_name, min_played, key_passes, assists, shots, xg, xa, match_id)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id, match_id) DO UPDATE SET 
                                    full_name = %s,
                                    min_played = %s,
                                    key_passes = %s,
                                    assists = %s,
                                    shots = %s,
                                    xg = %s,
                                    xa = %s
                            """
