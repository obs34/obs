import customtkinter as ctk
from interface.interface.page_versement import AppVersement
from interface.interface.page_catalogue import AppCatalogue

class Application(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Mon Application")
        self.geometry("900x600")

        # Configuration du conteneur principal
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Ici, ajoute sticky="nsew" pour remplir tout l'espace :
        self.container = ctk.CTkFrame(self)
        self.container.grid(row=0, column=0, sticky="nsew")

        # Configure le conteneur pour remplir toute la fenêtre
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)

        self.pages = {}

        for PageClass in (AppVersement, AppCatalogue):
            page_name = PageClass.__name__
            page = PageClass(master=self.container, app=self)
            self.pages[page_name] = page
            page.grid(row=0, column=0, sticky="nsew")  # important ici sticky="nsew"

        self.show_page("AppVersement")


    def show_page(self, page_name):
        page = self.pages[page_name]
        page.tkraise()

if __name__ == "__main__":
    app = Application()
    app.mainloop()
