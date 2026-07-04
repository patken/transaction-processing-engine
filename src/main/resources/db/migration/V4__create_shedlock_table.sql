-- ShedLock's lock store (ADR-006): one row per named scheduled job, so exactly one
-- instance runs each recovery job per cycle across a multi-node deployment.
-- TIMESTAMPTZ (not naive TIMESTAMP) for the same timezone-correctness reason as V2 (B4).
CREATE TABLE shedlock (
    name       VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMPTZ  NOT NULL,
    locked_at  TIMESTAMPTZ  NOT NULL,
    locked_by  VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);
