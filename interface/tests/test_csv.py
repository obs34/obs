import tkinter as tk
from tkinter import ttk
import pandas as pd

# Exemple de DataFrame
data = {
    'Nom': ['Alice', 'Bob', 'Charlie'],
    'Âge': [25, 30, 35],
    'Ville': ['Paris', 'Lyon', 'Marseille']
}
df = pd.DataFrame(data)

# Fonction pour mettre à jour le DataFrame
def update_dataframe():
    for i in range(len(df)):
        df.at[i, 'Nom'] = entries_nom[i].get()
        df.at[i, 'Âge'] = entries_age[i].get()
        df.at[i, 'Ville'] = entries_ville[i].get()
    print(df)

# Création de la fenêtre principale
root = tk.Tk()
root.title("Modifier un DataFrame avec Tkinter")

# Création des widgets pour chaque cellule du DataFrame
entries_nom = []
entries_age = []
entries_ville = []

for i in range(len(df)):
    tk.Label(root, text="Nom").grid(row=i, column=0)
    entry_nom = tk.Entry(root)
    entry_nom.insert(0, df.at[i, 'Nom'])
    entry_nom.grid(row=i, column=1)
    entries_nom.append(entry_nom)

    tk.Label(root, text="Âge").grid(row=i, column=2)
    entry_age = tk.Entry(root)
    entry_age.insert(0, df.at[i, 'Âge'])
    entry_age.grid(row=i, column=3)
    entries_age.append(entry_age)

    tk.Label(root, text="Ville").grid(row=i, column=4)
    entry_ville = tk.Entry(root)
    entry_ville.insert(0, df.at[i, 'Ville'])
    entry_ville.grid(row=i, column=5)
    entries_ville.append(entry_ville)

# Bouton pour mettre à jour le DataFrame
update_button = tk.Button(root, text="Mettre à jour le DataFrame", command=update_dataframe)
update_button.grid(row=len(df), column=0, columnspan=6)

# Lancer la boucle principale de Tkinter
root.mainloop()
