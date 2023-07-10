CREATE TABLE crime_log(
	dr_no 		int		NOT NULL,
	date_prtd 	date	NOT NULL,
	time_occ 	int,
	area 		int,
	area_name 	varchar(25),
	rpt_dist_no int,
	part_1_2 	int,
	crm_cd 		int,
	crm_cd_desc text,
	mocodes 	text,
	mocodes_description text,
	vict_age 	int,
	vict_sex 	char(1),
	vict_desc 	char(1),
	premise_cd 	int,
	premise_desc text,
	weapon_used_cd int,
	weapon_desc text,
	status 		varchar(10),
	status_desc text,
	crm_cd_1 	int,
	location 	text,
	lat 		float,
	lon 		float,
	Primary Key(dr_no)
)