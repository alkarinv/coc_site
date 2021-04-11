update war_attack set war_id = (select w.id FROM war w INNER JOIN war_clan wc on wc.war_id = w.id INNER JOIN war_player wp ON wp.war_clan_id = wc.id INNER JOIN war_attack wa on wa.war_player_id = wp.id);


select w.id FROM war w INNER JOIN war_clan wc on wc.war_id = w.id INNER JOIN war_player wp ON wp.war_clan_id = wc.id limit 10;


select wa.id, w.id FROM war w INNER JOIN war_clan wc on wc.war_id = w.id INNER JOIN war_player wp ON wp.war_clan_id = wc.id INNER JOIN war_attack wa on wa.war_player_id = wp.id

.mode csv
.output data/cwl_clans/clans_2021_03_48000018.csv
select distinct(tag) from clan_history where war_league = 48000018;

## Getting missed attacks
select count(attacker_tag), wc.war_id, w.war_type,  wc.tag, wp.tag, sum(wa.stars), sum(wa.destruction_percentage)/COUNT(attacker_tag)  from war_player as wp LEFT OUTER join war_attack as wa on wa.war_player_id
= wp.id JOIN war_clan as wc on wc.id = wp.war_clan_id JOIN war as w ON wc.war_id = w.id where wc.tag = "#8ULL0ULU" group by wp.id  ;