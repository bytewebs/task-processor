-- seed/seed_tasks.sql

-- Insert sample test data
INSERT INTO tasks (payload, status)
SELECT 
    'Test task ' || i,
    'pending'
FROM generate_series(1, 20) as i;
