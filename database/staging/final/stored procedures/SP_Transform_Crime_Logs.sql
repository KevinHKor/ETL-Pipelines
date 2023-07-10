CREATE PROCEDURE sp_map_mocodes_to_crime(days_loaded)
DO $$
DECLARE
	--
BEGIN
	CREATE TEMP TABLE mocode_to_descriptions
	AS
		WITH cte AS (
		SELECT dr_no, string_to_array(mocodes, ' ') as mocodes_arr
		FROM crime_logs
		)
		SELECT cl.dr_no, array_agg(mcl.description ORDER BY mcl.description) as mo_descriptions
		FROM cte AS cl
		LEFT JOIN mo_codes_lookup AS mcl
			ON mcl.code_id = any(cl.mocodes_arr::int[])
		GROUP BY cl.dr_no;

	INSERT INTO final.crime_logs
	SELECT 	cl.dr_no, 
			cl.date_rptd, 
			cl.time_occ, 
			cl.area, 
			cl.area_name, 
			cl.rpt_dist_no,
			cl.crm_cd, 
			cl.crm_cd_desc,
			cl.mocodes, 
			array_to_string(ctd.mo_descriptions, ', ') as mo_descriptions, 
			cl.vict_age, 
			cl.vict_sex, 
			cl.vict_descent, 
			cl.premis_cd,
			cl.premis_desc, 
			cl.weapon_used_cd, 
			cl.weapon_desc, 
			cl.status, 
			cl.status_desc, 
			cl.crm_cd_1,
			cl.location, 
			cl.lat, 
			cl.lon
	FROM crime_logs cl
	JOIN mocode_to_descriptions as ctd
		ON cl.dr_no=ctd.dr_no
EXCEPTION
	--
END;
$$
LANGUAGE plpgsql;    
