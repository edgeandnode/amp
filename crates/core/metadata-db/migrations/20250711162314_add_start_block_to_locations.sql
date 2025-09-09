-- Add the start_block column
ALTER TABLE locations ADD COLUMN start_block BIGINT;

-- Update existing NULLs to 0
UPDATE locations SET start_block = 0 WHERE start_block IS NULL;

-- Make the column non-nullable and set a default
ALTER TABLE locations
  ALTER COLUMN start_block SET DEFAULT 0,
  ALTER COLUMN start_block SET NOT NULL;
