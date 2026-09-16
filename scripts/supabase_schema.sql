-- ==========================================================
-- HeartBeat Schema for Supabase
-- Run this SQL in your Supabase SQL Editor
-- ==========================================================

-- 1. Create table for heartbeat logs
CREATE TABLE IF NOT EXISTS public.heartbeats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alias TEXT NOT NULL,
    source TEXT DEFAULT 'heartbeat-daemon',
    status TEXT DEFAULT 'alive',
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW()) NOT NULL
);

-- 2. Create index on created_at and alias for fast query & cleanup
CREATE INDEX IF NOT EXISTS idx_heartbeats_alias ON public.heartbeats (alias);
CREATE INDEX IF NOT EXISTS idx_heartbeats_created_at ON public.heartbeats (created_at DESC);

-- 3. Row Level Security (RLS) Configuration
ALTER TABLE public.heartbeats ENABLE ROW LEVEL SECURITY;

-- 4. Idempotent Policy Creation
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'heartbeats' AND policyname = 'Allow insert heartbeats'
    ) THEN
        CREATE POLICY "Allow insert heartbeats" ON public.heartbeats FOR INSERT WITH CHECK (true);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'heartbeats' AND policyname = 'Allow read heartbeats'
    ) THEN
        CREATE POLICY "Allow read heartbeats" ON public.heartbeats FOR SELECT USING (true);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'heartbeats' AND policyname = 'Allow delete heartbeats'
    ) THEN
        CREATE POLICY "Allow delete heartbeats" ON public.heartbeats FOR DELETE USING (true);
    END IF;
END
$$;
