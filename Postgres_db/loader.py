import pandas as pd
import psycopg2

class ExcelToPostgresLoader:
    TARGET_COLUMNS = [
        "Дата", "Подразделение", "Операция", "Культура",
        "За день, га", "С начала операции, га", "Вал за день, ц", "Вал с начала, ц"
    ]

    def __init__(self, db_config: dict):
        self.db_config = db_config

    def clean_text(self, text):
        if pd.isna(text):
            return None
        return str(text).encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")

    def parse_excel(self, filepath: str) -> pd.DataFrame:
        df_raw = pd.read_excel(filepath, header=None, engine="openpyxl")

        header_rows = []
        for idx, row in df_raw.iterrows():
            row_values = row.astype(str).tolist()
            if "Дата" in row_values and "Подразделение" in row_values:
                header_rows.append(idx)

        all_dataframes = []
        for i in range(len(header_rows)):
            start_row = header_rows[i] + 1
            end_row = header_rows[i + 1] - 1 if i < len(header_rows) - 1 else len(df_raw) - 1
            df_block = df_raw.iloc[start_row:end_row + 1, [1,2,3,4,5,6,7,8]].copy()
            df_block.columns = self.TARGET_COLUMNS
            df_block.dropna(how="all", inplace=True)
            df_block = df_block[df_block["Операция"].notna()]
            all_dataframes.append(df_block)

        return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame(columns=self.TARGET_COLUMNS)

    def upload_to_postgres(self, df: pd.DataFrame):
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO operations
                ("Дата", "Подразделение", "Операция", "Культура",
                "За день, га", "С начала операции, га", "Вал за день, ц", "Вал с начала, ц")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            record = (
                self.clean_text(row["Дата"]),
                self.clean_text(row["Подразделение"]),
                self.clean_text(row["Операция"]),
                self.clean_text(row["Культура"]),
                row["За день, га"],
                row["С начала операции, га"],
                row["Вал за день, ц"],
                row["Вал с начала, ц"]
            )
            cursor.execute(insert_query, record)

        conn.commit()
        cursor.close()
        conn.close()
