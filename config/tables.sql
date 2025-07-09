CREATE TABLE parsed_articles (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE,
    query TEXT,
    title TEXT,
    snippet TEXT,
    engine TEXT,
    published TIMESTAMP,
    score FLOAT,
    text TEXT,
    error TEXT,
    inserted_at TIMESTAMP DEFAULT now()
);


CREATE TABLE failed_articles (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE,
    query TEXT,
    title TEXT,
    snippet TEXT,
    engine TEXT,
    published TIMESTAMP,
    score FLOAT,
    text TEXT,
    error TEXT,
    inserted_at TIMESTAMP DEFAULT now()
);
