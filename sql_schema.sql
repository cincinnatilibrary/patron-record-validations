-- https://www.sqlite.org/lockingv3.html
PRAGMA locking_mode=SHARED;

-- Setting the journal mode to WAL (Write-Ahead Logging) for better concurrency
PRAGMA journal_mode=WAL;

-- set the cache to consume ~50 MB of memory (4 KB page size is the default)
PRAGMA cache_size = 12800;

-- set reasonable analysis limit
PRAGMA analysis_limit=400;

-- maintain data integrity by enforcing foreign key constraints
-- PRAGMA foreign_keys = ON;
-- run a check ...
-- PRAGMA foreign_key_check;

--

CREATE TABLE IF NOT EXISTS patrons (
    patron_record_id INTEGER PRIMARY KEY,
    barcode1 TEXT,
    patron_record_num INTEGER,
    ptype_code INTEGER,
    home_library_code TEXT,
    campus_code TEXT,
    create_timestamp_utc INTEGER,
    delete_timestamp_utc INTEGER,
    update_timestamp_utc INTEGER,
    expire_timestamp_utc INTEGER,
    active_timestamp_utc INTEGER,
    claims_returned_total INTEGER,
    owed_amt_cents INTEGER,
    mblock_code TEXT,
    highest_level_overdue_num INTEGER,
    num_revisions INTEGER
);

CREATE INDEX IF NOT EXISTS idx_patrons_patron_record_id on patrons (
    patron_record_id
);

CREATE INDEX IF NOT EXISTS idx_patrons_update_timestamp_utc on patrons (
    update_timestamp_utc
);

ANALYZE patrons;
ANALYZE idx_patrons_patron_record_id;
ANALYZE idx_patrons_update_timestamp_utc;

--

CREATE TABLE IF NOT EXISTS patron_json_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    patron_record_id INTEGER,
    json_data_type TEXT, -- identifiers, phone_numbers, emails, etc: add more as we need
    json_data TEXT,
    update_timestamp_utc INTEGER DEFAULT (strftime('%s', 'now')),
    FOREIGN KEY (patron_record_id) REFERENCES patrons(patron_record_id)
    UNIQUE (patron_record_id, json_data_type)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_patron_json_data_unique_composite ON patron_json_data(
    patron_record_id, 
    json_data_type
);


CREATE INDEX IF NOT EXISTS idx_patron_json_data_composite ON patron_json_data(
    patron_record_id,
    json_data_type,
    update_timestamp_utc
);

ANALYZE patron_json_data;
ANALYZE idx_patron_json_data_composite;
ANALYZE idx_patron_json_data_unique_composite;

--

CREATE TABLE IF NOT EXISTS patron_json_changes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    patron_record_id INTEGER,
    json_data_type TEXT,
    diff TEXT,
    update_timestamp_utc INTEGER DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_patron_json_changes_composite ON patron_json_changes(
    patron_record_id,
    json_data_type,
    update_timestamp_utc
);

CREATE INDEX IF NOT EXISTS idx_patron_json_changes_diff_is_not_null_partial_index ON patron_json_changes(
    diff
) WHERE diff IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_patron_json_changes_diff_is_null_partial_index ON patron_json_changes(
    diff
) WHERE diff IS NULL;

ANALYZE patron_json_changes;
ANALYZE idx_patron_json_changes_composite;
ANALYZE idx_patron_json_changes_diff_is_not_null_partial_index;
ANALYZE idx_patron_json_changes_diff_is_null_partial_index;

--

CREATE TRIGGER IF NOT EXISTS trg_patron_json_data_update
AFTER UPDATE ON patron_json_data
FOR EACH ROW
WHEN 
    OLD.json_data != NEW.json_data                  -- json data is NOT matching
    -- AND OLD.json_data_type = NEW.json_data_type     -- data types ARE matching
    -- AND OLD.patron_record_id = NEW.patron_record_id -- patron_record_id ARE matching*
                                                    -- (NOTE this may not be necessary, but... )
BEGIN
    INSERT INTO patron_json_changes(
        json_data_type,
        patron_record_id, 
        diff
    )
    VALUES(
        NEW.json_data_type,
        NEW.patron_record_id,
        json_diff(
            NEW.json_data, 
            OLD.json_data
        )
    );
END;

-- INSERTS would be new values without previous values
CREATE TRIGGER IF NOT EXISTS trg_patron_json_data_insert
AFTER INSERT ON patron_json_data
BEGIN
    INSERT INTO patron_json_changes (
        json_data_type, 
        patron_record_id, 
        diff
    )
    VALUES (
        NEW.json_data_type,
        NEW.patron_record_id,
        NULL -- there are no changes, so insert null
        -- this was the previous method ... -- json_diff(NEW.json_data, '[]')
    );
END;

--

CREATE VIEW IF NOT EXISTS patron_recent_changes_view AS
with recent_changes as (
  with patron_data as (
    select
      patron_record_id,
      json_data_type,
      diff,
      update_timestamp_utc
    from
      patron_json_changes
    where
      diff is not null
    order by
      update_timestamp_utc DESC
  )
  select
    patron_record_id,
    max(update_timestamp_utc) as max_update_timestamp_utc,
    json_group_array(
      json_object(
        'update',
        datetime(update_timestamp_utc, 'unixepoch', 'localtime'),
        'json_data_type',
        json_data_type,
        'diff',
        json(diff)
      )
    ) as previous_updates
  from
    patron_data
  group by
    patron_record_id
)
SELECT
  p.patron_record_id,
  p.barcode1,
  p.patron_record_num,
  p.ptype_code,
  (
    SELECT
      json_group_array(
        json_object(
          'update',
          datetime(
            sorted_pj.update_timestamp_utc,
            'unixepoch',
            'localtime'
          ),
          'data_type',
          sorted_pj.json_data_type,
          'data',
          json(sorted_pj.json_data)
        )
      )
    FROM
      (
        SELECT
          pj.update_timestamp_utc,
          pj.json_data_type,
          pj.json_data
        FROM
          patron_json_data AS pj
        WHERE
          pj.patron_record_id = p.patron_record_id
        ORDER BY
          CASE
            WHEN pj.json_data_type = 'patron_address_json' THEN 1
            WHEN pj.json_data_type = 'identifiers_json' THEN 2
            WHEN pj.json_data_type = 'emails_json' THEN 3
            ELSE 4
          END
      ) AS sorted_pj
  ) AS data_fields,
  recent_changes.previous_updates
FROM
  patrons AS p
  join recent_changes on recent_changes.patron_record_id = p.patron_record_id
WHERE
  p.delete_timestamp_utc IS NULL
ORDER BY
  recent_changes.max_update_timestamp_utc DESC
;

--

PRAGMA optimize;

-- VACUUM;