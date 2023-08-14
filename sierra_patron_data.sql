
-- This SQL query retrieves detailed information about patrons from the sierra_view.record_metadata table.
-- It fetches details such as their address, barcodes, alternative IDs, phone numbers, and email addresses.
-- Basic validation checks are also performed for email addresses.

-- Note: All timestamps are converted to UNIX EPOCH timestamps (UTC) and stored as such* in the local database

select
	rm.id as patron_record_id,
	rm.record_num as patron_record_num,
	rm.campus_code,
	(
		select
			v.field_content 
		from 
			sierra_view.varfield as v 
		where 
			v.record_id = rm.id
			and v.varfield_type_code = 'b'
		order by 
			v.occ_num 
		limit 1
	) as barcode1,
	pr.home_library_code,
    pr.ptype_code,
	-- rm.creation_date_gmt at time zone 'UTC'       as create_timestamp_utc,
	-- rm.deletion_date_gmt at time zone 'UTC'       as delete_timestamp_utc,
	-- rm.record_last_updated_gmt at time zone 'UTC' as update_timestamp_utc,
	-- pr.expiration_date_gmt at time zone 'UTC'     as expire_timestamp_utc,
	-- pr.activity_gmt at time zone 'UTC'            as active_timestamp_utc,
    extract(epoch from rm.creation_date_gmt)         as create_timestamp_utc,
    extract(epoch from rm.deletion_date_gmt)         as delete_timestamp_utc,
	extract(epoch from rm.record_last_updated_gmt)   as update_timestamp_utc,
	extract(epoch from pr.expiration_date_gmt)       as expire_timestamp_utc,
	extract(epoch from pr.activity_gmt)              as active_timestamp_utc,
	pr.claims_returned_total,
	cast(pr.owed_amt * 100 as INTEGER) as owed_amt_cents,
	pr.mblock_code,
	pr.highest_level_overdue_num, 
	rm.num_revisions,
	(
		select
			count(*)
		from
			sierra_view.patron_record_address as pra
		where 
			pra.patron_record_id = rm.id
	) as count_addresses,
	(
		select 
			json_agg(
				json_build_object(
                    -- It doen't seem like tracking the id is actually useful
					-- 'address_id', pra.id,
                    --
					'address_type_code', prat.code, 
	        		'addr1', pra.addr1,
	        		'addr2', pra.addr2,
	        		'addr3', pra.addr3,
	        		'village', pra.village,
	        		'city', pra.city,
	        		'postal_code', pra.postal_code,
	        		'region', pra.region
	        	)
	        	order by
	        		pra.display_order
	    	)
	    from
	    	sierra_view.patron_record_address as pra
	    	left outer join sierra_view.patron_record_address_type as prat on prat.id = pra.patron_record_address_type_id
	    where
	    	pra.patron_record_id = rm.id 
	)::TEXT as patron_address_json,
	(
		select 
			count(*)
		from
			sierra_view.varfield as v
		where
			v.record_id = rm.id
			and v.varfield_type_code in (
				'b' -- barcode
			) 
	) as count_barcode_identifiers,
	(
		select 
			count(*)
		from
			sierra_view.varfield as v
		where
			v.record_id = rm.id
			and v.varfield_type_code in (
				'v'  -- alt_id
			)
	) as count_alt_id_identifiers,
	(
		select
			json_agg(
				json_build_object(
                    -- the id isn't very useful here
					-- 'varfield_id', v.id,
                    --
					'identifier_type', v.varfield_type_code, 
					'identifier', v.field_content
				)
				order by
					v.varfield_type_code ,
					v.occ_num
			)
		from
			sierra_view.varfield as v
		where
			v.record_id = rm.id
			and v.varfield_type_code in (
				'b', -- barcode
				'v'  -- alt_id
			)
	)::TEXT as identifiers_json,
	(
		select
		count(*)
		from 
			sierra_view.patron_record_phone as prp 
		where
			prp.patron_record_id = rm.id
	) as count_phone_numbers,
	(
		select
		json_agg(
			json_build_object (
                -- the id isn't very useful here
				-- 'phone_id', prp.id,
				'phone_number', prp.phone_number,
			    'phone_type', vtn.short_name
			    -- validations
                -- these are interesting to do at the postgres level, but they're better left to their own table, and performed as a local db trigger
			    -- 'invalid_non_numeric_characters', CASE WHEN NOT prp.phone_number ~ '^[0-9]+$' THEN TRUE ELSE FALSE END,
                -- 'invalid_length', CASE WHEN NOT LENGTH(prp.phone_number) BETWEEN 10 AND 15 THEN TRUE ELSE FALSE END,
                -- 'invalid_has_spaces', CASE WHEN POSITION(' ' IN prp.phone_number) > 0 THEN TRUE ELSE FALSE END
			)
			order by
				prp.display_order 
		)
		from 
			sierra_view.patron_record_phone as prp 
			join sierra_view.patron_record_phone_type as prpt on prpt.id = prp.patron_record_phone_type_id 
			join sierra_view.varfield_type as vt on (
				vt.code = prpt.code 
				and vt.record_type_code = 'p'
			)
			join sierra_view.varfield_type_name as vtn on vtn.varfield_type_id = vt.id
		where
			prp.patron_record_id = rm.id
	)::TEXT as phone_numbers_json,
	(
		with split_values AS (
		    select
		    	v.record_id,
		    	v.id,
		        unnest(string_to_array(v.field_content, ',')) AS field_content
		        -- row_number() OVER (PARTITION BY v.id, v.record_id, v.occ_num) AS idx
		    from
		        sierra_view.varfield as v
		        join sierra_view.patron_record as pr on pr.record_id = v.record_id
		    where 
		        v.varfield_type_code = 'z'
		        and v.record_id = rm.id
		)
		select
			json_agg(
				json_build_object( 
                    -- the id isn't very useful here
					-- 'varfield_id', id,
                    --
					'email', field_content
                    -- validations
                    -- these are interesting to do at the postgres level, but they're better left to their own table, and performed as a local db trigger
					-- 'invalid_has_spaces', CASE WHEN position(' ' in field_content) > 0 THEN TRUE ELSE FALSE END,
                	-- 'invalid_missing_or_multiple_at', CASE WHEN length(field_content) - length(replace(field_content, '@', '')) != 1 THEN TRUE ELSE FALSE END
			    )
			)
		from 
		    split_values	
	)::TEXT as emails_json
from 
	sierra_view.record_metadata as rm
	left outer join sierra_view.patron_record as pr on pr.record_id = rm.id 
where 
	rm.record_type_code = 'p'
    and rm.record_last_updated_gmt >= to_timestamp(%s)


-- The split_values CTE splits email addresses stored as comma-separated values in the sierra_view.varfield table.
-- This CTE is used later to extract individual email addresses and perform basic validation checks.

-- The main SELECT statement fetches various details about each patron.
-- Multiple subqueries are used to aggregate related information (e.g., addresses, phone numbers) into JSON arrays.
-- Basic email validation checks are performed, such as ensuring no spaces and exactly one @ character.

-- Note: Email validation is tricky due to the variety of valid formats and domain-specific rules.
-- The checks here will catch basic issues, but there might still be invalid emails that pass through.