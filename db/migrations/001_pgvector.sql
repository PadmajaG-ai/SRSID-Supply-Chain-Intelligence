-- ─────────────────────────────────────────────────────────────────────────────
-- SRSID  —  db/migrations/001_pgvector.sql
-- Enable pgvector extension and add embedding column to vendors table.
-- Run once in Supabase SQL Editor before running rag/build_index.py
-- ─────────────────────────────────────────────────────────────────────────────

-- Step 1: Enable pgvector extension (available on all Supabase plans)
CREATE EXTENSION IF NOT EXISTS vector;

-- Step 2: Add embedding column to vendors table
-- all-MiniLM-L6-v2 produces 384-dimensional vectors
ALTER TABLE vendors
    ADD COLUMN IF NOT EXISTS embedding vector(384);

-- Step 3: Add vendor_text column to store the text chunk that was embedded
-- Useful for debugging and for returning context to Flan-T5
ALTER TABLE vendors
    ADD COLUMN IF NOT EXISTS vendor_text TEXT;

-- Step 4: Create an IVFFlat index for fast approximate nearest-neighbour search
-- lists=100 is appropriate for 2,541 vendors (rule of thumb: sqrt(n_rows))
-- Using cosine distance to match all-MiniLM-L6-v2 which produces normalised vectors
CREATE INDEX IF NOT EXISTS idx_vendors_embedding
    ON vendors
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);

-- ─────────────────────────────────────────────────────────────────────────────
-- Verify setup
-- ─────────────────────────────────────────────────────────────────────────────
-- After running, verify with:
--   SELECT column_name, data_type
--   FROM information_schema.columns
--   WHERE table_name = 'vendors'
--   AND column_name IN ('embedding', 'vendor_text');
-- Should return 2 rows.
