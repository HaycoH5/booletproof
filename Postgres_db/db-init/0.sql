-- Main table
CREATE TABLE operations (
    id SERIAL PRIMARY KEY,
    "Дата" TEXT,
    "Подразделение" TEXT,
    "Операция" TEXT,
    "Культура" TEXT,
    "За день, га" NUMERIC,
    "С начала операции, га" NUMERIC,
    "Вал за день, ц" NUMERIC,
    "Вал с начала, ц" NUMERIC
);
