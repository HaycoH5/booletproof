import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import psycopg2
import os
from config import DB_CONFIG
from loader import ExcelToPostgresLoader

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Agro DB Viewer")
        self.loader = ExcelToPostgresLoader(DB_CONFIG)
        self._sort_column = None
        self._sort_reverse = False

        self.create_widgets()
        self.load_data()

    def create_widgets(self):
        top_frame = tk.Frame(self.root)
        top_frame.pack(pady=10, fill=tk.X)

        tk.Button(top_frame, text="📂 Загрузить Excel", command=self.load_excel).pack(side=tk.LEFT, padx=5)
        tk.Button(top_frame, text="🗑️ Очистить таблицу", command=self.clear_table).pack(side=tk.LEFT, padx=5)
        tk.Button(top_frame, text="🔄 Обновить", command=self.load_data).pack(side=tk.LEFT, padx=5)

        search_frame = tk.Frame(self.root)
        search_frame.pack(pady=5, fill=tk.X)
        tk.Label(search_frame, text="🔍 Поиск:").pack(side=tk.LEFT)
        self.search_var = tk.StringVar()
        search_entry = tk.Entry(search_frame, textvariable=self.search_var)
        search_entry.pack(side=tk.LEFT, padx=5)
        tk.Button(search_frame, text="Фильтровать", command=self.apply_filter).pack(side=tk.LEFT, padx=5)
        tk.Button(search_frame, text="Сброс", command=self.load_data).pack(side=tk.LEFT, padx=5)

        tree_frame = tk.Frame(self.root)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        self.tree = ttk.Treeview(tree_frame, show="headings")
        self.tree.bind("<Double-1>", self.on_double_click)

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

    def load_data(self, filter_text=None):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            df = pd.read_sql("SELECT * FROM operations ORDER BY id DESC", conn)
            conn.close()

            if filter_text:
                df = df[df.apply(lambda row: row.astype(str).str.contains(filter_text, case=False).any(), axis=1)]

            self.tree.delete(*self.tree.get_children())

            if not df.empty:
                self.tree["columns"] = list(df.columns)
                for col in df.columns:
                    self.tree.heading(col, text=col, command=lambda _col=col: self.sort_by_column(_col))
                    self.tree.column(col, width=120, anchor=tk.W)

                for _, row in df.iterrows():
                    self.tree.insert("", tk.END, values=list(row))
            else:
                self.tree["columns"] = ["Пусто"]
                self.tree.heading("Пусто", text="Нет данных")

        except Exception as e:
            messagebox.showerror("Ошибка загрузки", str(e))

    def sort_by_column(self, col):
        if col == self._sort_column:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = col
            self._sort_reverse = False

        items = [(self.tree.set(k, col), self.tree.item(k, "values")) for k in self.tree.get_children('')]

        try:
            items.sort(key=lambda t: float(t[0]) if t[0] not in ("", "None", "nan") else float('-inf'), reverse=self._sort_reverse)
        except ValueError:
            items.sort(key=lambda t: t[0], reverse=self._sort_reverse)

        self.tree.delete(*self.tree.get_children())
        for _, values in items:
            self.tree.insert("", tk.END, values=values)
    def apply_filter(self):
        text = self.search_var.get().strip()
        self.load_data(filter_text=text)

    def load_excel(self):
        filepath = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if filepath:
            try:
                df = self.loader.parse_excel(filepath)
                self.loader.upload_to_postgres(df)
                messagebox.showinfo("Успех", "Данные успешно загружены!")
                self.load_data()
            except Exception as e:
                messagebox.showerror("Ошибка загрузки Excel", str(e))

    def clear_table(self):
        confirm = messagebox.askyesno("Подтверждение", "Вы уверены, что хотите удалить все записи?")
        if confirm:
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()
                cur.execute("TRUNCATE operations RESTART IDENTITY;")
                conn.commit()
                cur.close()
                conn.close()
                messagebox.showinfo("Готово", "Все записи удалены и ID сброшены.")
                self.load_data()
            except Exception as e:
                messagebox.showerror("Ошибка", str(e))

    def on_double_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        item_id = self.tree.focus()
        values = self.tree.item(item_id, "values")
        if values:
            messagebox.showinfo("Строка", "\n".join(f"{col}: {val}" for col, val in zip(self.tree["columns"], values)))

if __name__ == "__main__":
    import time

    # Сплэш-окно с анимацией
    splash = tk.Tk()
    splash.title("Загрузка")
    splash_width = 300
    splash_height = 150
    screen_width = splash.winfo_screenwidth()
    screen_height = splash.winfo_screenheight()
    x = (screen_width // 2) - (splash_width // 2)
    y = (screen_height // 2) - (splash_height // 2)
    splash.geometry(f"{splash_width}x{splash_height}+{x}+{y}")
    splash.resizable(False, False)

    splash_label = tk.Label(splash, text="🔄 Загрузка приложения...", font=("Arial", 12))
    splash_label.pack(pady=10)

    progress = ttk.Progressbar(splash, orient="horizontal", length=200, mode="determinate")
    progress.pack(pady=10)

    def animate():
        for i in range(0, 101, 5):
            progress["value"] = i
            splash.update()
            time.sleep(0.05)

    animate()
    time.sleep(0.5)
    splash.destroy()

    root = tk.Tk()
    window_width = 1200
    window_height = 600
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width // 2) - (window_width // 2)
    y = (screen_height // 2) - (window_height // 2)
    root.geometry(f"{window_width}x{window_height}+{x}+{y}")
    root.minsize(900, 400)
    app = App(root)
    root.mainloop()
