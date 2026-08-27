-- Run once for an existing schedule-enabled D1 database before deploying the duration-enabled scheduler.
ALTER TABLE schedules ADD COLUMN duration_minutes INTEGER NOT NULL DEFAULT 0;
