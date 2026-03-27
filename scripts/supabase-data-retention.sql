-- =============================================================================
-- Umami Data Retention: Aggregate (Monthly) + Purge
-- =============================================================================
-- Run this in the Supabase SQL Editor.
-- It creates monthly aggregate tables, a cleanup function, and a daily cron.
-- Raw data older than 90 days is aggregated into monthly summaries then deleted.
-- The Umami dashboard continues to work for recent (last 90 days) data.
-- Aggregated historical data is available via direct SQL queries.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. AGGREGATE TABLES (MONTHLY)
-- ---------------------------------------------------------------------------

-- Monthly overview stats per website
CREATE TABLE IF NOT EXISTS website_stats_monthly (
  website_id UUID NOT NULL,
  month      DATE NOT NULL,  -- first day of month
  pageviews  BIGINT NOT NULL DEFAULT 0,
  visitors   BIGINT NOT NULL DEFAULT 0,
  visits     BIGINT NOT NULL DEFAULT 0,
  bounces    BIGINT NOT NULL DEFAULT 0,
  totaltime  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (website_id, month)
);

-- Monthly event-dimension metrics
CREATE TABLE IF NOT EXISTS event_metrics_monthly (
  website_id   UUID         NOT NULL,
  month        DATE         NOT NULL,  -- first day of month
  event_type   INTEGER      NOT NULL DEFAULT 1,
  metric_type  VARCHAR(50)  NOT NULL,
  metric_value VARCHAR(500) NOT NULL,
  views        BIGINT NOT NULL DEFAULT 0,
  visitors     BIGINT NOT NULL DEFAULT 0,
  visits       BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (website_id, month, event_type, metric_type, metric_value)
);

-- Monthly session-dimension metrics
CREATE TABLE IF NOT EXISTS session_metrics_monthly (
  website_id   UUID         NOT NULL,
  month        DATE         NOT NULL,  -- first day of month
  metric_type  VARCHAR(50)  NOT NULL,
  metric_value VARCHAR(250) NOT NULL,
  sessions     BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (website_id, month, metric_type, metric_value)
);

-- Indexes for querying
CREATE INDEX IF NOT EXISTS idx_website_stats_monthly_month
  ON website_stats_monthly (month);
CREATE INDEX IF NOT EXISTS idx_event_metrics_monthly_type
  ON event_metrics_monthly (website_id, metric_type, month);
CREATE INDEX IF NOT EXISTS idx_session_metrics_monthly_type
  ON session_metrics_monthly (website_id, metric_type, month);

-- ---------------------------------------------------------------------------
-- 2. AGGREGATE AND PURGE FUNCTION
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION aggregate_and_purge(
  retention_days INTEGER DEFAULT 90,
  batch_size     INTEGER DEFAULT 10000
)
RETURNS TABLE (
  stats_rows_inserted   BIGINT,
  event_rows_inserted   BIGINT,
  session_rows_inserted BIGINT,
  events_deleted        BIGINT,
  sessions_deleted      BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
  cutoff_date TIMESTAMPTZ;
  v_stats_inserted   BIGINT := 0;
  v_event_inserted   BIGINT := 0;
  v_session_inserted BIGINT := 0;
  v_events_deleted   BIGINT := 0;
  v_sessions_deleted BIGINT := 0;
  v_rows             BIGINT;
BEGIN
  cutoff_date := NOW() - (retention_days || ' days')::INTERVAL;

  RAISE NOTICE 'Cutoff date: %', cutoff_date;
  RAISE NOTICE 'Starting monthly aggregation...';

  -- =========================================================================
  -- STEP 1: Aggregate website_stats_monthly
  -- =========================================================================
  INSERT INTO website_stats_monthly (website_id, month, pageviews, visitors, visits, bounces, totaltime)
  SELECT
    t.website_id,
    t.month,
    COALESCE(SUM(t.c), 0)::BIGINT AS pageviews,
    COUNT(DISTINCT t.session_id)::BIGINT AS visitors,
    COUNT(DISTINCT t.visit_id)::BIGINT AS visits,
    COALESCE(SUM(CASE WHEN t.c = 1 THEN 1 ELSE 0 END), 0)::BIGINT AS bounces,
    COALESCE(SUM(EXTRACT(EPOCH FROM (t.max_time - t.min_time)))::BIGINT, 0) AS totaltime
  FROM (
    SELECT
      website_id,
      session_id,
      visit_id,
      DATE_TRUNC('month', created_at)::date AS month,
      COUNT(*) AS c,
      MIN(created_at) AS min_time,
      MAX(created_at) AS max_time
    FROM website_event
    WHERE created_at < cutoff_date
      AND event_type != 2
    GROUP BY website_id, session_id, visit_id, DATE_TRUNC('month', created_at)
  ) t
  GROUP BY t.website_id, t.month
  ON CONFLICT (website_id, month) DO UPDATE SET
    pageviews = website_stats_monthly.pageviews + EXCLUDED.pageviews,
    visitors  = website_stats_monthly.visitors  + EXCLUDED.visitors,
    visits    = website_stats_monthly.visits    + EXCLUDED.visits,
    bounces   = website_stats_monthly.bounces   + EXCLUDED.bounces,
    totaltime = website_stats_monthly.totaltime + EXCLUDED.totaltime;

  GET DIAGNOSTICS v_stats_inserted = ROW_COUNT;
  RAISE NOTICE 'website_stats_monthly: % rows upserted', v_stats_inserted;

  -- =========================================================================
  -- STEP 2: Aggregate event_metrics_monthly
  -- =========================================================================

  -- url_path (pageviews, event_type != 2)
  INSERT INTO event_metrics_monthly (website_id, month, event_type, metric_type, metric_value, views, visitors, visits)
  SELECT website_id, DATE_TRUNC('month', created_at)::date, 1, 'url_path', url_path,
    COUNT(*)::BIGINT, COUNT(DISTINCT session_id)::BIGINT, COUNT(DISTINCT visit_id)::BIGINT
  FROM website_event
  WHERE created_at < cutoff_date AND event_type != 2
  GROUP BY website_id, DATE_TRUNC('month', created_at), url_path
  ON CONFLICT (website_id, month, event_type, metric_type, metric_value) DO UPDATE SET
    views = event_metrics_monthly.views + EXCLUDED.views,
    visitors = event_metrics_monthly.visitors + EXCLUDED.visitors,
    visits = event_metrics_monthly.visits + EXCLUDED.visits;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_event_inserted := v_event_inserted + v_rows;

  -- referrer_domain
  INSERT INTO event_metrics_monthly (website_id, month, event_type, metric_type, metric_value, views, visitors, visits)
  SELECT website_id, DATE_TRUNC('month', created_at)::date, 1, 'referrer_domain', referrer_domain,
    COUNT(*)::BIGINT, COUNT(DISTINCT session_id)::BIGINT, COUNT(DISTINCT visit_id)::BIGINT
  FROM website_event
  WHERE created_at < cutoff_date AND event_type != 2
    AND referrer_domain IS NOT NULL AND referrer_domain != ''
  GROUP BY website_id, DATE_TRUNC('month', created_at), referrer_domain
  ON CONFLICT (website_id, month, event_type, metric_type, metric_value) DO UPDATE SET
    views = event_metrics_monthly.views + EXCLUDED.views,
    visitors = event_metrics_monthly.visitors + EXCLUDED.visitors,
    visits = event_metrics_monthly.visits + EXCLUDED.visits;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_event_inserted := v_event_inserted + v_rows;

  -- page_title
  INSERT INTO event_metrics_monthly (website_id, month, event_type, metric_type, metric_value, views, visitors, visits)
  SELECT website_id, DATE_TRUNC('month', created_at)::date, 1, 'page_title', page_title,
    COUNT(*)::BIGINT, COUNT(DISTINCT session_id)::BIGINT, COUNT(DISTINCT visit_id)::BIGINT
  FROM website_event
  WHERE created_at < cutoff_date AND event_type != 2
    AND page_title IS NOT NULL AND page_title != ''
  GROUP BY website_id, DATE_TRUNC('month', created_at), page_title
  ON CONFLICT (website_id, month, event_type, metric_type, metric_value) DO UPDATE SET
    views = event_metrics_monthly.views + EXCLUDED.views,
    visitors = event_metrics_monthly.visitors + EXCLUDED.visitors,
    visits = event_metrics_monthly.visits + EXCLUDED.visits;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_event_inserted := v_event_inserted + v_rows;

  -- hostname
  INSERT INTO event_metrics_monthly (website_id, month, event_type, metric_type, metric_value, views, visitors, visits)
  SELECT website_id, DATE_TRUNC('month', created_at)::date, 1, 'hostname', hostname,
    COUNT(*)::BIGINT, COUNT(DISTINCT session_id)::BIGINT, COUNT(DISTINCT visit_id)::BIGINT
  FROM website_event
  WHERE created_at < cutoff_date AND event_type != 2
    AND hostname IS NOT NULL AND hostname != ''
  GROUP BY website_id, DATE_TRUNC('month', created_at), hostname
  ON CONFLICT (website_id, month, event_type, metric_type, metric_value) DO UPDATE SET
    views = event_metrics_monthly.views + EXCLUDED.views,
    visitors = event_metrics_monthly.visitors + EXCLUDED.visitors,
    visits = event_metrics_monthly.visits + EXCLUDED.visits;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_event_inserted := v_event_inserted + v_rows;

  -- event_name (custom events, event_type = 2)
  INSERT INTO event_metrics_monthly (website_id, month, event_type, metric_type, metric_value, views, visitors, visits)
  SELECT website_id, DATE_TRUNC('month', created_at)::date, 2, 'event_name', event_name,
    COUNT(*)::BIGINT, COUNT(DISTINCT session_id)::BIGINT, COUNT(DISTINCT visit_id)::BIGINT
  FROM website_event
  WHERE created_at < cutoff_date AND event_type = 2
    AND event_name IS NOT NULL AND event_name != ''
  GROUP BY website_id, DATE_TRUNC('month', created_at), event_name
  ON CONFLICT (website_id, month, event_type, metric_type, metric_value) DO UPDATE SET
    views = event_metrics_monthly.views + EXCLUDED.views,
    visitors = event_metrics_monthly.visitors + EXCLUDED.visitors,
    visits = event_metrics_monthly.visits + EXCLUDED.visits;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_event_inserted := v_event_inserted + v_rows;

  -- tag
  INSERT INTO event_metrics_monthly (website_id, month, event_type, metric_type, metric_value, views, visitors, visits)
  SELECT website_id, DATE_TRUNC('month', created_at)::date, 1, 'tag', tag,
    COUNT(*)::BIGINT, COUNT(DISTINCT session_id)::BIGINT, COUNT(DISTINCT visit_id)::BIGINT
  FROM website_event
  WHERE created_at < cutoff_date AND event_type != 2
    AND tag IS NOT NULL AND tag != ''
  GROUP BY website_id, DATE_TRUNC('month', created_at), tag
  ON CONFLICT (website_id, month, event_type, metric_type, metric_value) DO UPDATE SET
    views = event_metrics_monthly.views + EXCLUDED.views,
    visitors = event_metrics_monthly.visitors + EXCLUDED.visitors,
    visits = event_metrics_monthly.visits + EXCLUDED.visits;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_event_inserted := v_event_inserted + v_rows;

  RAISE NOTICE 'event_metrics_monthly: % total rows upserted', v_event_inserted;

  -- =========================================================================
  -- STEP 3: Aggregate session_metrics_monthly
  -- =========================================================================

  -- browser
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'browser', s.browser,
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.browser IS NOT NULL AND s.browser != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), s.browser
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  -- os
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'os', s.os,
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.os IS NOT NULL AND s.os != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), s.os
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  -- device
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'device', s.device,
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.device IS NOT NULL AND s.device != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), s.device
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  -- country
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'country', s.country,
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.country IS NOT NULL AND s.country != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), s.country
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  -- region
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'region', s.region,
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.region IS NOT NULL AND s.region != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), s.region
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  -- city
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'city', s.city,
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.city IS NOT NULL AND s.city != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), s.city
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  -- language
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'language', LOWER(LEFT(s.language, 2)),
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.language IS NOT NULL AND s.language != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), LOWER(LEFT(s.language, 2))
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  -- screen
  INSERT INTO session_metrics_monthly (website_id, month, metric_type, metric_value, sessions)
  SELECT we.website_id, DATE_TRUNC('month', we.created_at)::date, 'screen', s.screen,
    COUNT(DISTINCT s.session_id)::BIGINT
  FROM website_event we
  INNER JOIN session s ON we.session_id = s.session_id AND we.website_id = s.website_id
  WHERE we.created_at < cutoff_date AND we.event_type != 2
    AND s.screen IS NOT NULL AND s.screen != ''
  GROUP BY we.website_id, DATE_TRUNC('month', we.created_at), s.screen
  ON CONFLICT (website_id, month, metric_type, metric_value) DO UPDATE SET
    sessions = session_metrics_monthly.sessions + EXCLUDED.sessions;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_session_inserted := v_session_inserted + v_rows;

  RAISE NOTICE 'session_metrics_monthly: % total rows upserted', v_session_inserted;
  RAISE NOTICE 'Aggregation complete. Starting purge...';

  -- =========================================================================
  -- STEP 4: Delete old raw data in batches
  -- =========================================================================

  -- 4a. Delete old revenue
  LOOP
    DELETE FROM revenue
    WHERE revenue_id IN (
      SELECT revenue_id FROM revenue WHERE created_at < cutoff_date LIMIT batch_size
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXIT WHEN v_rows = 0;
  END LOOP;
  RAISE NOTICE 'Deleted old revenue rows';

  -- 4b. Delete old event_data
  LOOP
    DELETE FROM event_data
    WHERE event_data_id IN (
      SELECT event_data_id FROM event_data WHERE created_at < cutoff_date LIMIT batch_size
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXIT WHEN v_rows = 0;
  END LOOP;
  RAISE NOTICE 'Deleted old event_data rows';

  -- 4c. Delete old session_data
  LOOP
    DELETE FROM session_data
    WHERE session_data_id IN (
      SELECT session_data_id FROM session_data WHERE created_at < cutoff_date LIMIT batch_size
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXIT WHEN v_rows = 0;
  END LOOP;
  RAISE NOTICE 'Deleted old session_data rows';

  -- 4d. Delete old website_event
  LOOP
    DELETE FROM website_event
    WHERE event_id IN (
      SELECT event_id FROM website_event WHERE created_at < cutoff_date LIMIT batch_size
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_events_deleted := v_events_deleted + v_rows;
    EXIT WHEN v_rows = 0;
  END LOOP;
  RAISE NOTICE 'Deleted % website_event rows', v_events_deleted;

  -- 4e. Delete orphaned sessions
  LOOP
    DELETE FROM session
    WHERE session_id IN (
      SELECT s.session_id FROM session s
      WHERE s.created_at < cutoff_date
        AND NOT EXISTS (SELECT 1 FROM website_event we WHERE we.session_id = s.session_id)
      LIMIT batch_size
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_sessions_deleted := v_sessions_deleted + v_rows;
    EXIT WHEN v_rows = 0;
  END LOOP;
  RAISE NOTICE 'Deleted % orphaned session rows', v_sessions_deleted;

  RAISE NOTICE 'Purge complete.';

  RETURN QUERY SELECT
    v_stats_inserted,
    v_event_inserted,
    v_session_inserted,
    v_events_deleted,
    v_sessions_deleted;
END;
$$;

-- ---------------------------------------------------------------------------
-- 3. SCHEDULE WITH PG_CRON (runs daily at 3 AM UTC)
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
  'umami-aggregate-purge',
  '0 3 * * *',
  $$SELECT * FROM aggregate_and_purge()$$
);

-- ---------------------------------------------------------------------------
-- 4. FIRST RUN & VERIFICATION
-- ---------------------------------------------------------------------------
-- Run initial aggregation + purge:
--   SELECT * FROM aggregate_and_purge();
--
-- Reclaim disk space (run via psql, not SQL Editor):
--   VACUUM FULL website_event;
--   VACUUM FULL session;
--
-- Verify:
--   SELECT count(*) FROM website_stats_monthly;
--   SELECT count(*) FROM event_metrics_monthly;
--   SELECT count(*) FROM session_metrics_monthly;
--   SELECT count(*) FROM website_event WHERE created_at < NOW() - INTERVAL '90 days';
--   SELECT * FROM cron.job;
--
-- Sample queries:
--   SELECT month, pageviews, visitors, visits
--   FROM website_stats_monthly
--   WHERE website_id = 'your-uuid'
--   ORDER BY month;
--
--   SELECT metric_value AS url_path, SUM(views) AS total_views
--   FROM event_metrics_monthly
--   WHERE website_id = 'your-uuid' AND metric_type = 'url_path'
--   GROUP BY metric_value ORDER BY total_views DESC LIMIT 20;
