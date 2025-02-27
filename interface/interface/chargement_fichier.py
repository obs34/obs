from tkinter import filedialog, messagebox
import pandas as pd

class chargementFichiers:
    def __init__(self, file_label, sheets_list):
        self.file_label = file_label
        self.sheets_list = sheets_list

    def load_excel(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
        if file_path:
            self.file_label.configure(text=file_path.split("/")[-1])
            try:
                excel_data = pd.ExcelFile(file_path)
                sheet_names = excel_data.sheet_names
                output = "\n".join([f" {sheet}" for sheet in excel_data.sheet_names])
                self.sheets_list.delete("0.0", "end")
                self.sheets_list.insert("0.0", output)
            except Exception as e:
                messagebox.showerror("Error", f"Error loading Excel file: {e}")