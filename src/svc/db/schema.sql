DO $$ BEGIN
    CREATE TYPE casino_transaction_type AS ENUM ('win', 'bet');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY,
    user_id text NOT NULL,
    transaction_type casino_transaction_type NOT NULL,
    amount integer NOT NULL,
    ts timestamp NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_transactions_ts ON transactions(ts);