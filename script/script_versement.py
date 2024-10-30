import csv
import datetime
import getpass
import glob
import os
import re
import pathlib
import random
import shutil
import sys
import time
import traceback
from typing import Optional
from cryptography.fernet import Fernet

import ipywidgets as widgets
from IPython.display import display, clear_output

import numpy as np
import pandas as pd
import psycopg2
import unidecode
from tqdm import tqdm

class Fonctions3:
    '''
    Script de versement vers pgAdmin
    !/usr/bin/env python
    coding: utf-8
    3e version du script de versement
    '''
    
    ##############################################################################################################################
    ############# Constantes
    ##############################################################################################################################   

    colnames_val = ['id', 'id_versement', 'annee', 'echelle', 'code_entite', 'id_var', 'id_mod', 'valeur']
    colnames_var = ['id_var', 'nom_var', 'joli_nom_var', 'var_regroupement', 'lib_long_var']
    colnames_mod = ['id_mod', 'nom_mod', 'joli_nom_mod', 'mod_regroupement', 'lib_long_mod']
    colnames_dict= ['id_versement', 'nom_table', 'annee', 'echelle', 'theme', 'source', 'commentaire', 'url']

    nom_dossier_brutes_csv = 'fichiers_brutes_csv' #chemin = le nom du dossier où se trouve le fichier csv à traiter
    nom_dossier_fichiers_traites = 'fichiers_traites'

    def __init__(self):
        self.con, self.nom_schema = self.connexion_observatoire()
        
        self.nom_excel = None # à remplir dans le notebook
        self.annee = None # à remplir dans le notebook

        self.source = None # à remplir dans le notebook
        self.base = None # à remplir dans le notebook
        self.theme = None # à remplir dans le notebook

        # à modifier aussi dans importe()
        self.nom_table_var = f't_{self.nom_schema}_var'.lower()
        self.nom_table_mod = f't_{self.nom_schema}_mod'.lower()
        self.nom_table_dict = f't_{self.nom_schema}_dict'.lower()

        self.choix_input = None
        self.id = None # Pour éviter de passer None à la fonction traitement_val

        self.sep = ';'

    # Le décorateur @property est utilisé pour définir des méthodes dans une classe qui se comportent comme des attributs.
    @property
    def nom_table(self):
        return f'{self.theme}_{self.base}_{self.source}'
    
    @property
    def chemin_csv_val(self):
        return f'{self.nom_dossier_fichiers_traites}/{self.nom_table}.csv'

    ##############################################################################################################################
    ############# Connexion à l'observatoire
    ##############################################################################################################################

    def connexion_observatoire(self):
        """
        Demande à l'utilisateur de choisir l'observatoire auquel il est rattaché.
        Puis établit une connexion à la base de données de l'observatoire choisi.

        Parameters
        ----------
        Nothing.

        Returns
        -------
        con : psycopg2.extensions.connection
        str : nom du schéma de l'observatoire choisi
        """

        observatoires = {
            '1': {
                'nom_obs': "l'ODH dans bdsociohab",
                'database_pg': "bdsociohab",
                'nom_utilisateur': "admoddh",
                'nom_host': "s934",
                'port': "5441",
                'nom_schema': "socio_hab_test"
            },
            '2': {
                'nom_obs': "la MDDP",
                'database_pg': "bdsociohab",
                'nom_utilisateur': "admmddep",
                'nom_host': "s934",
                'port': "5441",
                'nom_schema': "test"
            },
            '3': {
                'nom_obs': "l'ODH",
                'database_pg': "obs",
                'nom_utilisateur': "hab",
                'nom_host': "s934",
                'port': "5441",
                'nom_schema': "hab"
            },
            '4': {
                'nom_obs': "l'Observatoire de test",
                'database_pg': "obs",
                'nom_utilisateur': "test",
                'nom_host': "s934",
                'port': "5441",
                'nom_schema': "test"
            }
        }
        time.sleep(1) # Pour éviter que les messages se mélangent
        while True:
            print("Demande connexion!")
            print("Liste des observatoires disponibles :")
            for observatoire, params in observatoires.items():
                print(f"{observatoire} : {params['nom_obs']}")
            
            observatoire = input("Veuillez choisir l'observatoire auquel vous êtes rattaché.e : ")

            if observatoire in observatoires:
                params = observatoires[observatoire]
                mot_de_passe = getpass.getpass(f"Entrez votre mot de passe pour {params['nom_obs']}: ")

                try:
                    con = psycopg2.connect(database=params['database_pg'], user=params['nom_utilisateur'],
                            password=mot_de_passe, host=params['nom_host'], port=params['port'])
                    print("Connexion réussie!")
                    print((f"Vous avez choisi l'observatoire \033[1m{params['nom_obs']}\033[0m.\n" 
                        f"Vous vous apprêtez actuellement à faire votre versement dans la base de données \033[1m{params['database_pg']}\033[0m et le schéma \033[1m{params['nom_schema']}\033[0m."))
                    return con, params['nom_schema']
                except psycopg2.Error as error:
                    print("Erreur lors de la connexion à pgAdmin (mot de passe incorrect). Veuillez ressaisir le mot de passe.")

            else:
                print("Veuillez répondre par un entier correspondant à l'un des observatoires proposés.")

    ##############################################################################################################################
    ############# Traitement données complet
    ##############################################################################################################################

    def traitement(self):
        """Fonction mère qui appelle toutes les fonctions nécessaires pour le traitement des données.

        1. Input: Choix de l'échelle.
        2. Input: Choix du remplissage manuel ou automatique des libellés et catégories de regroupement.
        3. Création des dossiers temporaires.
        4. Importation et lecture des feuilles du fichier Excel.
        5. Importation des données importantes de mod, var et dict.
        6. Traitement du dictionnaire.
        7. Traitement des variables.
        8. Traitement des valeurs et des modalités.

        Parameters
        ----------
        self.nom_excel (notebook): str
            Chemin d'accès au fichier Excel à traiter.
        self.annee (notebook): int
            Année de récolte des données.
        self.source (notebook): str
            Source des données.
        self.base (notebook): str
            Nom de la base de donnée s'il y en a un.
        self.theme (notebook): str
            Thème des données dans la base de données pgAdmin.

        Returns
        -------
        Nothing.
        """

        ####### CHOIX ECHELLE #######
        echelles = {
            1: 'Commune',
            2: 'EPCI',
            3: 'IRIS',
            4: 'Canton',
            5: 'Logement',
            6: 'Parcelle',
            7: 'Section cadastrale',
            8: 'Département',
            9: 'Autre'
        }

        echelle_ok = False
        print("Liste des observatoires disponibles :")
        for numero, echelle in echelles.items():
            print(f"{numero} : {echelle}")
        time.sleep(1) # Pour éviter que les messages se mélangent
        while not echelle_ok:
            echelle_numero = input("Veuillez choisir l'échelle correspondant à l'échelle de vos données :")
            try:
                echelle_int = int(echelle_numero)
                if echelle_int in echelles and echelle_int != 9:
                    self.echelle = echelles[echelle_int]
                    print(f"Vous avez choisi l'échelle {self.echelle}.")
                    echelle_ok = True
                elif echelle_int == 9:
                    self.echelle = input("Veuillez saisir l'échelle correspondant à l'échelle de vos données :")
                    time.sleep(1)
                    print(f"Vous avez choisi l'échelle {self.echelle}.")
                    echelle_ok = True
                else:
                    print('Réponse invalide')
            except ValueError:
                print('Réponse invalide')

        ####### CHOIX REMPLISSAGE MANUEL / AUTO #######
        input_ok = False
        while not input_ok:
            self.choix_input = input("""Souhaitez-vous saisir les libellés et catégorie de regroupement pour toutes vos nouvelles modalités ? (O/N)""").upper()
            if self.choix_input == 'N' or self.choix_input == 'O':
                input_ok = True
                print(f"Vous avez répondu : {self.choix_input}")
                if self.choix_input == 'N':
                    self.choix_input = False
            else:
                print("Répondez par : O ou N")
                input_ok = False    

        ####### CREATION DOSSIERS TEMPORAIRES #######
        # Ajout d'"ID" aux dossiers vars pour les sauvegarder. Cela peut être utilse si on a déjà fais des modifications dedans.
        date_heure = datetime.datetime.now().strftime('%Y%j%H%M%S')
        self.nom_dossier_vars = 'vars_' + date_heure

        self.chemin_csv_var = f'{self.nom_dossier_vars}/new{self.nom_table_var}.csv'
        self.chemin_csv_mod = f'{self.nom_dossier_vars}/new{self.nom_table_mod}.csv'      
        self.chemin_csv_dict = f'{self.nom_dossier_vars}/new{self.nom_table_dict}.csv'

        os.mkdir(self.nom_dossier_vars)
        os.mkdir(self.nom_dossier_fichiers_traites)
        os.mkdir(self.nom_dossier_brutes_csv)
        print('Fichiers temporaires créés.')

        ####### IMPORTATION ET LECTURE DU FICHIER EXCEL #######
        print('Importation des feuilles Excel en cours...')
        # Création des csv des feuilles de l'excel dans le dossier fichiers_brutes_csv
        excel_file = pd.ExcelFile(str(self.nom_excel), engine='openpyxl')    # pb engine = 'openpyxl'
        for sheet in excel_file.sheet_names:
            excel_feuille = pd.read_excel(excel_file,sheet_name=sheet, engine='openpyxl')  # pb engine = 'openpyxl'
            excel_feuille.to_csv('fichiers_brutes_csv/'+sheet.lower()+".csv",index=False,sep=self.sep, encoding='utf-8-sig')
            time.sleep(0.5)
        
        # https://stackoverflow.com/questions/6773584/how-are-glob-globs-return-values-ordered?newreg=87d884fd981b4445973a65d2690922c6
        # nom_feuilles = liste des csv présents dans le dossier fichiers_brutes_csv par ordre croissant de création.
        # Explication: glob.glob() liste dans le désordre et ça va avoir une mauvaise influence sur id_var, puis sur "order by id_var" en SQL.
        # Précision: fichiers_brutes_csv contient en csv chaque feuille du fichier Excel d'origine nommés par le nom de la feuille.
        nom_feuilles = sorted(glob.glob(self.nom_dossier_brutes_csv + '/*.csv'), key=os.path.getmtime)
        val_feuilles = [pd.read_csv(nom_feuille, sep=self.sep, encoding='utf-8-sig') for nom_feuille in nom_feuilles]
        nom_feuilles = [nom.replace(self.nom_dossier_brutes_csv, '')
                             .replace('//', '')
                             .replace('/', '')
                             .replace('\\', '')
                             .replace('.csv', '') for nom in nom_feuilles]
        
        feuilles = {nom: valeurs for nom, valeurs in zip(nom_feuilles, val_feuilles)}
        print('Importation des feuilles Excel terminée.')
        
        ####### IMPORTATION DES DONNEES DE MOD, VAR ET DICT #######
        self.dictionnaire_var, self.max_id_var = self.importe('var')
        self.dictionnaire_mod, self.max_id_mod = self.importe('mod')
        self.dictionnaire_dict, self.max_id_dict, self.ligne_elue = self.importe('dict', element=True)

        ####### TRAITEMENT DICTIONNAIRE #######
        self.max_id_dict += 1
        self.traitement_dict()
        print('Traitement du dictionnaire terminé.')

        ############## TRAITEMENT VAR ##########################
        self.dictionnaire_var = self.traitement_var(nom_feuilles)
        print('Traitement des variables terminé.')

        ############## TRAITEMENT VAL ET MOD ################
        # nom_feuilles_sans_accent_ni_escpace = self.replacingSpaceModa(nom_feuilles)
        table_finale_val = pd.DataFrame(columns = self.colnames_val)
        # Initialisation des IDs
        self.id, _ = self.importe('val')
        _, self.id_versement, _ = self.importe('dict')
        self.id_versement += 1
        self.nouvelles_mod = []
        for nom_feuille, feuille  in tqdm(feuilles.items()):
            # nom_var = nom_feuilles_sans_accent_ni_escpace[feuille]
            nom_var = nom_feuille
            print('_'*50)
            print('nom de la feuille en cours de traitement:', nom_var)
            feuille_traitee = self.traitement_val(nom_feuille, feuille)
            table_finale_val = pd.concat([table_finale_val, feuille_traitee])
        
        self.nouvelles_mod = pd.DataFrame(self.nouvelles_mod)
        # self.nouvelles_mod.fillna(None, inplace=True)
        self.nouvelles_mod.to_csv(self.chemin_csv_mod,index=False,sep=self.sep, encoding='utf-8-sig')
        # Lors du versement dans pg de la table valeur, il y a une complication 
        # avec la colonne valeur en double précision qui ne prend pas le valeurs vides ('').
        # Elle prend par contre en compte les None.
        # N'ayant pas trouvé d'autres solutions, on remplace les na en 'NULL' dans la table valeur
        # Puis on remplace les 'NULL' par None avec values = [x if x!='NULL' else None for x in row] dans insertion_donnees.
        table_finale_val.fillna('NULL', inplace=True) 
        pd.DataFrame(table_finale_val).to_csv(self.chemin_csv_val, index=False, encoding='utf-8-sig',sep=self.sep)  
        print('Traitement des valeurs et des modalités terminé.')
        self.jolie_print((f'Opération terminée: les fichiers temporaires ont été créé dans le dossier {self.nom_dossier_vars} \n'
                        'Vous pouvez désormais verser les données grâce à la cellule 10 ou les vérifier dans le dossier vars et fichiers_traites.'))


    ##############################################################################################################################
    ############# Checking variables
    ##############################################################################################################################

    def traitement_var(self, nom_feuilles):
        '''Fonction qui vérifie si les variables sont déjà présentes dans la table var.

        Trouve les variables qui n'ont pas encore été importées dans la base de donnée et les ajoute dans un fichier CSV.

        Parameters
        ----------
        nom_feuilles : list
            Liste des noms des feuilles du fichier Excel importé.
        
        Returns
        -------
        self.dictionnaire_var | dict_new_var : dict
            Dictionnaire des variables déjà dans la tables et à importer.
            Avec clé: noms des variables et valeur: id.
        
        '''
        var_a_rajouter = pd.DataFrame(columns=self.colnames_var)
        # Initialisation des variables
        nouvelles_var = []
        # Parcours des variables à vérifier
        for nom_var in nom_feuilles:
            print(f'variable recherchée : {nom_var}')
            nom_var_propre = unidecode.unidecode(nom_var).lower()
            if(nom_var_propre in self.dictionnaire_var.keys()):
                print(self.styleP('trouvée', color='green'))                            
            else:
                print(self.styleP('nouvelle', color='blue'))
                self.max_id_var += 1
                if self.choix_input:
                    lib_long_var = input('Veuillez saisir le libellé long de cette variable : ')
                    if not lib_long_var:
                        lib_long_var = nom_var
                    joli_nom_var = input('Veuillez saisir un joli nom pour cette variable : ')
                    if not joli_nom_var:
                        joli_nom_var = nom_var
                    var_regroupement = input('Veuillez saisir la catégorie de regroupement de cette variable : ')
                    if not var_regroupement:
                        var_regroupement = nom_var_propre
                else:
                    joli_nom_var = nom_var
                    lib_long_var = ''
                    var_regroupement = nom_var_propre

                # Création de la variable à rajouter
                var_a_rajouter = {
                    self.colnames_var[0] : self.max_id_var, #len(Variables)+1,  #len(Variables) nombre de variable dans la table var et +1 parce qu'on en rajoute
                    self.colnames_var[1] : nom_var_propre,
                    self.colnames_var[2] : joli_nom_var,
                    self.colnames_var[3] : var_regroupement,
                    self.colnames_var[4] : lib_long_var
                    # self.colnames_var[5] : self.source.lower()
                }
                nouvelles_var.append(var_a_rajouter)
                print(f'la variable {nom_var} a été insérée')

        # Enregistrement des nouvelles variables dans un fichier CSV
        nouvelles_var = pd.DataFrame(nouvelles_var)
        nouvelles_var.to_csv(self.chemin_csv_var,index=False,sep=self.sep, encoding='utf-8-sig')

        # Dictionnaire des noms des variables en clé et de leur id en valeur pour éviter de concaténer 2 df et de l'enregistrer en csv
        dict_new_var = {nouvelles_var.at[ligne, self.colnames_var[1]]: nouvelles_var.at[ligne, self.colnames_var[0]] for ligne in nouvelles_var.index}
        # 'unpack' puis rassemble les paires clé/valeur (la 2ème écrase la 1ère si les clés sont les mêmes)
        return {**self.dictionnaire_var, **dict_new_var} # self.dictionnaire_var.update(dict_new_var), self.dictionnaire_var | dict_new_var (ne marche dans la version actuelle d'ArcGIS)


    ##############################################################################################################################
    ############# Checking modalités
    ##############################################################################################################################

    def traitement_mod(self, feuille):      
        '''Fonction qui vérifie si les modalités sont déjà présentes dans la table mod.

        Trouve les modalités qui n'ont pas encore été importées dans la base de donnée et les ajoute dans un fichier CSV.

        Parameters
        ----------
        feuille : pd.DataFrame
            Feuille du fichier Excel importé.
        
        Returns
        -------
        dict_mod_feuile : dict
            Dictionnaire de toutes les modalités de la feuille.
            Avec clé: noms des modalités et valeur: id.
        
        '''
        mod_a_rajouter = pd.DataFrame(columns = self.colnames_mod)    
        #remplacement des caractères spéciaux
        #self.remplacerCaratereSpeciaux(feuille)
        
        #transformer les charactères en unicode et les premiers lettres en capitales (avec la fonction .title())
        # feuille.columns = self.replacingSpaceModa(feuille.columns)

        # Initialisation des variables
        dict_mod_feuile = {}
        # Parcours des modalités à vérifier   
        for nom_mod in feuille.columns[1:]: # 1: pour ne pas prendre en compte la première colonne qui sera code_entite
            nom_mod_propre = unidecode.unidecode(nom_mod).lower()
            print('Modalité recherchée :', nom_mod)
            if(nom_mod_propre in self.dictionnaire_mod.keys()):
                dict_mod_feuile[nom_mod_propre] = self.dictionnaire_mod[nom_mod_propre]
                print(self.styleP('trouvée', color= 'green'))
            else:
                print(self.styleP('nouvelle', color='blue'))
                self.max_id_mod += 1 # Créé à partie de la fonction importe
                if self.choix_input == 'OUI':
                    lib_long_mod = input('Veuillez saisir le libellé long de cette modalité: ')
                    if not lib_long_mod:
                        lib_long_mod = nom_mod
                    joli_nom_mod = input('Veuillez saisir un joli nom pour cette modalité: ')
                    if not joli_nom_mod:
                        joli_nom_mod = nom_mod
                    mod_regroupement = input('Veuillez saisir la catégorie de regroupement de cette modalité: ')
                    if not mod_regroupement:
                        mod_regroupement = nom_mod
                else:
                    joli_nom_mod = nom_mod
                    lib_long_mod = ''
                    mod_regroupement = nom_mod_propre

                # Création de la modalité à rajouter
                mod_a_rajouter = {
                    self.colnames_mod[0] : self.max_id_mod,
                    self.colnames_mod[1] : nom_mod_propre,
                    self.colnames_mod[2] : joli_nom_mod,
                    self.colnames_mod[3] : mod_regroupement,
                    self.colnames_mod[4] : lib_long_mod
                }
                self.nouvelles_mod.append(mod_a_rajouter)
                self.dictionnaire_mod = {**self.dictionnaire_mod, **{nom_mod_propre: self.max_id_mod}} # self.dictionnaire_mod | {nom_mod_propre: self.max_id_mod} (ne marche dans la version actuelle d'ArcGIS)
                dict_mod_feuile[nom_mod_propre] = self.max_id_mod


        # Retourne les modalités de la feuille
        return dict_mod_feuile


    ##############################################################################################################################
    ############# Traitement données
    ##############################################################################################################################

    def traitement_val(self, nom_feuille, feuille): 
        '''Fonction qui créer la table valeur sérialisée.

        Ligne par ligne puis colonne par colonne, elle récupère les valeurs de la feuille et les ajoute dans un dataframe 
        avec les métadonnées correspondantes.

        Parameters
        ----------
        nom_feuille : char
            Nom de la variable ou de la feuille.
        feuille : pd.DataFrame
            Feuille du fichier Excel importé.

        Returns
        -------
        feuille_traitee : pd.DataFrame
            Dataframe de la feuille traitée.
        '''
        feuille_traitee = pd.DataFrame(columns = self.colnames_val)
        dict_mod_feuile = self.traitement_mod(feuille)
        # => Il faut absolument que le code entité soit dans la première colonne de chaque fichier.
        feuille.rename(columns={feuille.columns[0]: 'code_entite'}, inplace = True)
        # Récupération les valeurs (toutes les lignes, à partir de la 2ème colonne)
        # Une recherche est plus rapide dans une liste que dans un df.
        valeurs = feuille.iloc[:, 1:len(dict_mod_feuile)+1].values
        new_rows = []
        # colnames_val = ['id', 'id_versement', 'annee', 'echelle', 'code_entite', 'id_var', 'id_mod', 'valeur']
        for ligne in range(len(feuille)):
            code_entite = feuille['code_entite'][ligne]
            # Pour chaque colonne de la feuille, on récupère le code de la modalité et sa place associée.
            for col, code_moda in enumerate(dict_mod_feuile.values()):
                self.id += 1 # Pour avoir un self.id unique
                new_row = {
                    self.colnames_val[0]: self.id,
                    self.colnames_val[1]: self.id_versement,
                    self.colnames_val[2]: self.annee,
                    self.colnames_val[3]: self.echelle,
                    self.colnames_val[4]: code_entite,
                    self.colnames_val[5]: self.dictionnaire_var[unidecode.unidecode(nom_feuille).lower()],
                    self.colnames_val[6]: code_moda,
                    self.colnames_val[7]: valeurs[ligne, col]
                }
                new_rows.append(new_row)
        feuille_traitee = pd.DataFrame.from_records(new_rows)
        return feuille_traitee
        
    ##############################################################################################################################
    ############# Traitement dictionnaire
    ##############################################################################################################################

    def traitement_dict(self):
        '''Fonction qui traite le dictionnaire.

        Si l'utilisateur à favorablement au remplissage manuel, lui demande de saisir les informations nécessaires.
        Sinon, il récupère les informations déjà présentes.

        Parameters
        ----------
        Nothing.

        Returns
        -------
        Nothing.
        '''
        nom_table = self.nom_table
        annee = self.annee
        echelle = self.echelle
        source = self.source
        theme = self.theme

        if self.choix_input == 'OUI':
            commentaire = input(f'Veuillez saisir un commentaire (par défault: {self.ligne_elue[6]}): ') if self.ligne_elue else input(f'Veuillez saisir un commentaire: ')
            URL = input(f'Veuillez saisir l\'URL (par défault: {self.ligne_elue[7]}): ') if self.ligne_elue else input(f'Veuillez saisir l\'URL: ')
        else:
            commentaire = self.ligne_elue[6] if self.ligne_elue else ''
            URL = self.ligne_elue[7] if self.ligne_elue else ''


        # Création du dictionnaire à rajouter
        dictionnaire_table = pd.DataFrame({
            self.colnames_dict[0]: self.max_id_dict,
            self.colnames_dict[1]: nom_table,
            self.colnames_dict[2]: annee,
            self.colnames_dict[3]: echelle,
            self.colnames_dict[4]: theme,
            self.colnames_dict[5]: source,
            self.colnames_dict[6]: commentaire,
            self.colnames_dict[7]: URL
        }, index=[0])
        dictionnaire_table.to_csv(self.chemin_csv_dict, index=False, sep=self.sep, encoding='utf-8-sig')

    ##################################################################################################################################
    ### importation des donées importantes
    ##################################################################################################################################

    def importe(self, table, element=False):
        '''Fonction qui importe les données importantes de mod, var et dict.

        Parameters
        ----------
        table : str
            Nom de la table à importer.

        Returns
        -------
        dictionnaire : dict
            Dictionnaire des données de la table.
        max_id : int
            Valeur maximale de l'id de la table.
        '''
        cur = self.con.cursor()
        col_names = getattr(self, f'colnames_{table}')
        nom_table = f"_{self.nom_schema}_{table}" if table != 'val' else self.nom_table
        if self.table_exists(nom_table):
            try:                   
                query = f"SELECT {nom_table}.{col_names[0]}, {nom_table}.{col_names[1]} FROM {self.nom_schema}.{nom_table}"
                cur.execute(query)
                lignes = cur.fetchall()
                dictionnaire = {ligne[1]: ligne[0] for ligne in lignes}
                elue = None
                if element and table=='dict':
                    query = f"SELECT * FROM {self.nom_schema}.{nom_table}"
                    cur.execute(query)
                    lignes = cur.fetchall()
                    for ligne in lignes[::-1]: # Inverse le résultat
                        if self.nom_table == ligne[1]:
                            elue = ligne # Prend la dernière ligne où il y a le nom de la table
                            break
                cur.close()
                if table == 'val':
                    if self.table_vide():
                        return 0, 1
                    else:
                        return max(dictionnaire.values()), max(dictionnaire.keys())+1
                elif table=='dict':
                    return dictionnaire, max(dictionnaire.values()), elue
                else:
                    return dictionnaire, max(dictionnaire.values())
            except Exception as e:
                print("Une erreur s'est produite :", e)
                self.con.rollback()
        else:
            if table == 'val':
                return 0, 1
            elif table=='dict':
                return {}, 0, None
            else:
                return {}, 0

    ##################################################################################################################################
    ### demander choix binaire
    ##################################################################################################################################

    def demander_choix_binaire(self, message):
        """
        Demande à l'utilisateur de faire un choix binaire (O/N) jusqu'à ce qu'une réponse valide soit donnée.
       
        Args:
            message (str): Le message à afficher pour demander à l'utilisateur.
           
        Returns:
            bool: True si l'utilisateur choisit 'O', False si l'utilisateur choisit 'N'.
        """
        input_ok = False
        while not input_ok:
            choix = input(message + " (O/N): ").strip().upper()
            if choix in ['O', 'N']:
                input_ok = True
                if choix == 'O':
                    print("Vous avez choisi: Oui")
                    return True
                else:
                    print("Vous avez choisi: Non")
                    return False
            else:
                print("Répondez par : O ou N")    


    #############################################################################################################################
    ############# Versement donnees 3
    #############################################################################################################################

    def versement(self):
        """Fonction mère qui verse les données créées par self.traitement() dans pgAdmin.

        1. Définition des chemins des fichiers à verser.
        2. Définition du livre répertoriant toutes les informations nécessaires à la création des tables.
        3. Pour chaque table:
            a. Création de la table si elle n'existe pas.
            b. Versement des données dans la table.

        Parameters
        ----------
        Nothing.

        Returns
        -------
        Nothing.
        """
        # Prends le dernier dossier 'vars' à avoir été créé.
        self.nom_dossier_vars = sorted(glob.glob('vars_*'), key=os.path.getmtime)[-1]
        self.chemin_csv_var = f'{self.nom_dossier_vars}/new{self.nom_table_var}.csv'
        self.chemin_csv_mod = f'{self.nom_dossier_vars}/new{self.nom_table_mod}.csv'      
        self.chemin_csv_dict = f'{self.nom_dossier_vars}/new{self.nom_table_dict}.csv'

        self.livre = {
            'dictionnaire': {
                'nom_table': self.nom_table_dict,
                'chemin': self.chemin_csv_dict,
                'colnames': self.colnames_dict,
                'exist': self.table_exists(self.nom_table_dict),
                'cles_primaire': self.colnames_dict[0],
                'cles_etrangeres': [],
                'table_reference': [],
                'dtype': {self.colnames_dict[0]: int, self.colnames_dict[1]: str, self.colnames_dict[2]: int,
                          self.colnames_dict[3]: str, self.colnames_dict[4]: str, self.colnames_dict[5]: str,
                          self.colnames_dict[6]: str, self.colnames_dict[7]: str}
            },
            'variable': {
                'nom_table': self.nom_table_var,
                'chemin': self.chemin_csv_var,
                'colnames': self.colnames_var,
                'exist': self.table_exists(self.nom_table_var),
                'cles_primaire': self.colnames_var[0],
                'cles_etrangeres': [],
                'table_reference': [],
                'dtype': {self.colnames_var[0]: int, self.colnames_var[1]: str, self.colnames_var[2]: str,
                          self.colnames_var[3]: str, self.colnames_var[4]: str, 'objectid': int}
            },
            'modalite': {
                'nom_table': self.nom_table_mod,
                'chemin': self.chemin_csv_mod,
                'colnames': self.colnames_mod,
                'exist': self.table_exists(self.nom_table_mod),
                'cles_primaire': self.colnames_mod[0],
                'cles_etrangeres': [],
                'table_reference': [],
                'dtype': {self.colnames_mod[0]: int, self.colnames_mod[1]: str, self.colnames_mod[2]: str,
                          self.colnames_mod[3]: str, self.colnames_mod[4]: str, 'objectid': int} #, self.colnames_mod[5]: str à cause de l'inscription dans ArcServer
            },
            'valeur': {
                'nom_table': self.nom_table,
                'chemin': self.chemin_csv_val,
                'colnames': self.colnames_val,
                'exist': self.table_exists(self.nom_table),
                'cles_primaire': self.colnames_val[0],
                'cles_etrangeres': [self.colnames_val[5], self.colnames_val[6], self.colnames_val[1]],
                'table_reference': [self.nom_table_var, self.nom_table_mod, self.nom_table_dict],
                'dtype': {self.colnames_val[0]: int, self.colnames_val[1]: int, self.colnames_val[2]: int,
                          self.colnames_val[3]: str, self.colnames_val[4]: str, self.colnames_val[5]: int,
                          self.colnames_val[6]: int, self.colnames_val[7]: float} #, self.colnames_val[7]: str
            }
        }

        try:
            self.con.autocommit = False # Désactiver l'autocommit pour utiliser une transaction.
        except:
            print('je sais pas... ProgrammingError: set_session cannot be used inside a transaction')
        try:
            print('Début du versement...')
            self.cur = self.con.cursor()
            tables = ['dictionnaire', 'variable', 'modalite', 'valeur']
            for table in tables:
                print('_'*100)
                print(f"Préparation de la table {table}...")
                if self.livre[table]['exist'] == False:
                    self.creation_table_pg(table)
                    print(f"La table {table} {self.livre[table]['nom_table']} est prête à être créée.")
                self.insertion_donnees(table)
                print(f"Les données sont prêtes à être insérées dans la table {table}.")
            self.con.commit()
            print(self.styleP("Versement terminé.", color="yellow", style="bold"))

        except(Exception, NameError, psycopg2.Error) as error :
            self.con.rollback()
            print(traceback.format_exc())
            print ("Erreur lors de l'insertion des données importées dans la table dans le try :", error)
            
        finally:
            self.con.autocommit = True
            self.cur.close()

    ##############################################################################################################################
    ############# Création table avec fichier complet
    ##############################################################################################################################

    def creation_table_pg(self, table):

        # Création de la requête de création de la table
        requete = f"CREATE TABLE {self.nom_schema}.{self.livre[table]['nom_table']}("
        lignes_des_colonnes = []
        for col in self.livre[table]['colnames']:
            type_colonne = self.livre[table]['dtype'][col]
            ligne = f"{col} {self.map_pandas_to_postgres_type(type_colonne)}"

            # Clé primaire
            if col == self.livre[table]['cles_primaire']:
                ligne += " PRIMARY KEY"

            lignes_des_colonnes.append(ligne)
        requete += ', '.join(lignes_des_colonnes)

        # Clés étrangères
        requetes_etrangeres = []
        if self.livre[table]['cles_etrangeres']:
            for cle_etrangere, table_referente in zip(self.livre[table]['cles_etrangeres'], self.livre[table]['table_reference']):
                requete_etrangere = f"CONSTRAINT fk_{table_referente} FOREIGN KEY({cle_etrangere}) REFERENCES {self.nom_schema}.{table_referente} ({cle_etrangere})"
                requetes_etrangeres.append(requete_etrangere)
            requete += ',\n' + ',\n'.join(requetes_etrangeres)

        requete += ')'

        # Création de la table
        # try:
        self.cur.execute(requete)
            # print(f"La table {self.livre[table]['nom_table']} a été créée avec succès dans pgAdmin \n")

        # except (Exception, psycopg2.Error) as error :
        #     print (f"Erreur lors de la création de la table {self.livre[table]['nom_table']} dans pgAdmin: ", error)
        #     print(requete)


    ##############################################################################################################################
    ############# insertion données
    ##############################################################################################################################
    def insertion_donnees(self, table): #changement
        """
        Données : fichierCsv est un fichier CSV dont on veut reproduire la structure sur une self.base de données
                con = une instance de la fonction connexion
                table = nom (en string) de la table où vont être insérées les données
                
        résultat : insere les données de fichierCSV dans la table "table", dans une self.base de données. 
                Affiche une phrase indiquant si les données ont été insérées ou pas
        remarques : cette fonction est dédiée au versement des tables modalités, variables (et "posséder" si elle est créée)
        """
        with open(self.livre[table]['chemin'], 'r', encoding="utf-8-sig") as file:
            reader = csv.reader(file, delimiter=self.sep)
            next(reader) # Skip the header row.
            tps1 = time.perf_counter()
            if table == 'valeur':
                # Sinon problème avec double précision
                values = [tuple(row[:7]) + ((None,) if row[7] == 'NULL' else (row[7],)) for row in reader] # Créer une liste de tuples à partir du fichier CSV
            else:
                values = [tuple(row) for row in reader]
            if values: # Si la table est non vide.
                # Insertion des données dans la table
                if(len(values[0])==5): 
                    # https://stackoverflow.com/questions/32071536/typeerror-sequence-item-0-expected-str-instance-bytes-found
                    args_str = b','.join(self.cur.mogrify("(%s,%s,%s,%s,%s)", x) for x in values)
                    # https://stackoverflow.com/questions/55033372/can-only-concatenate-str-not-bytes-to-str
                    self.cur.execute(f"INSERT INTO {self.livre[table]['nom_table']} VALUES " + args_str.decode())
                if(len(values[0])==6): 
                    args_str = b','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s)", x) for x in values)
                    self.cur.execute(f"INSERT INTO {self.livre[table]['nom_table']} VALUES " + args_str.decode())
                if(len(values[0])==8): 
                    args_str = b','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in values)
                    self.cur.execute(f"INSERT INTO {self.livre[table]['nom_table']} VALUES " + args_str.decode())
                tps2 = time.perf_counter()
                print("Le temps pour préparer le versement des données de la table {} a été de {:.3f} secondes".format(table, tps2 - tps1))

    ##############################################################################################################################
    ############# map pandas to sql type
    ##############################################################################################################################

    def map_pandas_to_postgres_type(self, pandas_type):
        # permet de convertir les types pandas en postgresql

        postgres_types = {
            int: 'INTEGER',
            # int32: 'INTEGER',
            # int64: 'INTEGER',
            float: 'DOUBLE PRECISION',
            # float32: 'DOUBLE PRECISION',
            # float64: 'DOUBLE PRECISION',
            str: 'TEXT',
            object: 'TEXT',
            datetime: 'DATE'
        }

        return postgres_types[pandas_type]

    ##################################################################################################################################
    ### liste schema
    ##################################################################################################################################

    def liste_schema(self):
        con = self.connexion()
        cur = con.cursor()
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = cur.fetchall()
        for schema in schemas:
            print(schema[0])
        cur.close()
        con.close()


    ##################################################################################################################################
    ### liste tables
    ##################################################################################################################################

    def liste_table(self, afficher=False):
        
        cur = self.con.cursor()
        cur.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{self.nom_schema}'
            -- AND table_name NOT LIKE '_%'
            AND table_name NOT LIKE 'indicateurs_descriptions%'
            AND table_type = 'BASE TABLE'
        """)
        tables = cur.fetchall()
        cur.close()
        liste = []

        if not tables:
            print(self.styleP('Aucune table présente dans ce schéma', style='bold', color= "blue"))
        else:
            print(self.styleP(f'Tables existantes dans le schéma "{self.nom_schema}" sur PgAdmin:', style='bold', color= "blue")) 
            for table in tables:
                liste.append(table[0])  
        liste.sort()
        if afficher:
            for nom_table in liste:
                print(nom_table)    

        return liste

    ##################################################################################################################################
    ### existence table
    ##################################################################################################################################
                                                                    
    def table_exists(self, nom_table):
        with self.con.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE  table_name = %s and table_schema = %s
                );
            """, (nom_table, self.nom_schema))
            table_existe = cursor.fetchone()[0]
            cursor.close()
            return table_existe
        
    ##################################################################################################################################
    ### table vide ?
    ##################################################################################################################################
                                                                    
    def table_vide(self):
        with self.con.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*) FROM {self.nom_schema}.{self.nom_table};
            """)
            count = cursor.fetchone()[0]
            cursor.close()
            return count == 0

   
    ##################################################################################################################################
    ### liste feuilles
    ##################################################################################################################################
        
    def liste_feuille(self, nom_excel):
        df = pd.ExcelFile(str(nom_excel), engine = 'openpyxl')   
        print(self.styleP('Variable(s) pour le jeu de données:', style='bold', color="blue"))
        for sheet in df.sheet_names:
            print(sheet)


    ##################################################################################################################################
    ### suppression des fichiers
    ##################################################################################################################################

    def suppression_fichiers(self, vars=False):
        """
        Supprime les fichiers temporaires et les dossiers temporaires.

        Parameters
        ----------
        vars : bool, optional
            Supprime les dossiers temporaires qui contiennent 'vars' dans leur nom, par défaut False.

        Returns
        -------
        None
        """
        dossiers = ['fichiers_brutes_csv',self.nom_dossier_fichiers_traites]
        if vars == True:
            choix = input('Voulez-vous supprimer les fichiers temporaires ? (O/N) ').upper()
            if choix == 'O':
                dossiers += glob.glob('vars*')
            elif choix == 'N':
                pass
            else:
                print('Choix incorrect.')
        for dossier in dossiers:
            try:
                shutil.rmtree(dossier)
                print(f"Le dossier {dossier} a été supprimé.")
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))

    ##################################################################################################################################
    ### suppression de table/versement
    ##################################################################################################################################

    def suppression_table(self, table_cible, id_versement_cible=None, garder_table=False):
        """
        Si id_versement_cible est renseigné, supprime seulement les entrées correspondantes à cet id_versement dans la table
        et dans le dictionnaire.
        Sinon, supprime une table et les entrées correspondantes dans la table de dictionnaire.
        

        Args:
            table_cible (str): Nom de la table à supprimer.
            id_versement_cible (int, optional): Id_versement à supprimer. Defaults to None.

        Returns:
            None
        """
        # Obtenir la liste des tables.
        requete = f"SELECT DISTINCT {self.colnames_dict[1]} FROM {self.nom_schema}.{self.nom_table_dict}"
        self.cur = self.con.cursor()
        self.cur.execute(requete)
        liste_table = [x[0] for x in self.cur.fetchall()]

        # Priorisation de la table cible
        liste_table.remove(table_cible)
        liste_table.insert(0, table_cible)

        # Initialisation des listes
        liste_var_del = []
        liste_mod_del = []
        liste_var_autre = []
        liste_mod_autre = []

        # Si id_versement_cible n'est pas renseigné, supprime toute la table.
        # Si id_versement_cible n'est pas renseigné, supprime toutes les lignes de la table (pas besoin de DROP CASCADE les vues).
        if not id_versement_cible:
            requete_dict = f"DELETE FROM {self.nom_table_dict} WHERE {self.colnames_dict[1]} = '{table_cible}'"
            requete_table = f"DROP TABLE {self.nom_schema}.{table_cible}"
            if garder_table:
                requete_table = f"DELETE FROM {self.nom_schema}.{table_cible}"
            
        else:
            requete_table = f"DELETE FROM {self.nom_schema}.{table_cible} WHERE {self.colnames_val[1]} = {id_versement_cible}"
            requete_dict = f"DELETE FROM {self.nom_table_dict} WHERE {self.colnames_dict[0]} = {id_versement_cible}"

            # Obtient tous les id_versement de la table.
            requete = f"SELECT DISTINCT {self.colnames_val[1]} FROM {self.nom_schema}.{table_cible}"
            self.cur = self.con.cursor()
            self.cur.execute(requete)
            liste_id_versement = [x[0] for x in self.cur.fetchall()]

            # Priorise l'id_versment cible.
            liste_id_versement.remove(id_versement_cible)
            liste_id_versement.insert(0, id_versement_cible)

            # Prend les id_mod et id_var pour chaque id_versement.
            for id_versement in liste_id_versement:
                requete = f"""
                SELECT DISTINCT {self.nom_table_var}.{self.colnames_var[0]}, {self.nom_table_mod}.{self.colnames_mod[0]}
                FROM {self.nom_schema}.{table_cible}
                JOIN {self.nom_table_var} ON {table_cible}.{self.colnames_var[0]} = {self.nom_table_var}.{self.colnames_var[0]}
                JOIN {self.nom_table_mod} ON {table_cible}.{self.colnames_mod[0]} = {self.nom_table_mod}.{self.colnames_mod[0]}
                WHERE {table_cible}.{self.colnames_val[1]} = {id_versement}
                ORDER BY {self.colnames_var[0]}, {self.colnames_mod[0]}
                """
                self.cur.execute(requete)
                liste_tuple = self.cur.fetchall()
                
                if id_versement == id_versement_cible:
                    liste_var_del = list(np.unique([x[0] for x in liste_tuple]))
                    liste_mod_del = list(np.unique([x[1] for x in liste_tuple]))
                else:
                    liste_var_autre += list(np.unique([x[0] for x in liste_tuple]))
                    liste_mod_autre += list(np.unique([x[1] for x in liste_tuple]))

            # Suppression des variables et modalités communes
            liste_var_del = [var for var in liste_var_del if var not in liste_var_autre]
            liste_mod_del = [mod for mod in liste_mod_del if mod not in liste_mod_autre]

            liste_table.remove(table_cible)

        # Récupération des variables et modalités pour chaque table
        for table in liste_table:
            requete = f"""
            SELECT DISTINCT {self.nom_table_var}.{self.colnames_var[0]}, {self.nom_table_mod}.{self.colnames_mod[0]}
            FROM {self.nom_schema}.{table}
            JOIN {self.nom_table_var} ON {table}.{self.colnames_var[0]} = {self.nom_table_var}.{self.colnames_var[0]}
            JOIN {self.nom_table_mod} ON {table}.{self.colnames_mod[0]} = {self.nom_table_mod}.{self.colnames_mod[0]}
            ORDER BY {self.colnames_var[0]}, {self.colnames_mod[0]}
            """
            self.cur.execute(requete)
            liste_tuple = self.cur.fetchall()
            
            if table == table_cible:
                liste_var_del = list(np.unique([x[0] for x in liste_tuple]))
                liste_mod_del = list(np.unique([x[1] for x in liste_tuple]))
            else:
                liste_var_autre += list(np.unique([x[0] for x in liste_tuple]))
                liste_mod_autre += list(np.unique([x[1] for x in liste_tuple]))

        # Suppression des variables et modalités communes
        liste_var_del = [var for var in liste_var_del if var not in liste_var_autre]
        liste_mod_del = [mod for mod in liste_mod_del if mod not in liste_mod_autre]
        # pg n'accepte une liste que sous la forme de tuple mais un tuple d'un élément sous Python est sous cette forme: (x,).
        # Donc on tranforme la liste en string en on enlève les crochets.
        liste_var_del = str(liste_var_del)[1:-1]
        liste_mod_del = str(liste_mod_del)[1:-1]

        # Suppression des id_var et id_mod.
        requete_var = f"DELETE FROM {self.nom_table_var} WHERE {self.colnames_var[0]} in ({liste_var_del})"
        requete_mod = f"DELETE FROM {self.nom_table_mod} WHERE {self.colnames_mod[0]} in ({liste_mod_del})"

        liste_requete = [requete_table, requete_dict, requete_var, requete_mod]
        if liste_var_del=='':
            liste_requete.remove(requete_var)
        if liste_mod_del=='':
            liste_requete.remove(requete_mod)
        # print(liste_requete)

        choix_utilisateur = self.demander_choix_binaire(f"""
    Souhaitez-vous supprimer la table/versement ?\nListe des requêtes:\n{liste_requete}""")
        if choix_utilisateur:
            try:
                self.con.autocommit = False # Désactiver l'autocommit pour utiliser une transaction.
            except:
                print('je sais pas... ProgrammingError: set_session cannot be used inside a transaction')
            if self.con.autocommit:
                print("self.con.autocommit:",self.con.autocommit)
            else:
                print("self.con.autocommit:",self.con.autocommit)
                # try:
                #     self.con.autocommit = False # Désactiver l'autocommit pour utiliser une transaction.
                # except:
                #     print('je sais pas... ProgrammingError: set_session cannot be used inside a transaction')
                try:
                    self.cur = self.con.cursor()
                    # Exécuter la requête pour supprimer la table
                    for requete in liste_requete:
                        print(f"Requête:\n{requete}")
                        with self.con.cursor() as cursor:
                            cursor.execute(requete)
                    self.con.commit()
                    print("Opération réussie.")
                except(Exception, NameError, psycopg2.Error) as error :
                    self.con.rollback()
                    print(traceback.format_exc())
                    print ("Erreur lors de la suppression des données de la table dans le try :", error)
                    
                finally:
                    self.con.autocommit = True
                    self.cur.close()

    ##################################################################################################################################
    ### DICTIONNAIRE 
    ##################################################################################################################################

    def dict(self):

        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

        # https://www.geeksforgeeks.org/how-to-print-an-entire-pandas-dataframe-in-python/

        dictionnaires = {'1': {'choix': "pour faire une recherche sur les modalités et les variables d'une table en particulier.",
                               'nom_table': None},
                        '2': {'choix': "pour faire une recherche sur la table des modalités.",
                                     'nom_table': self.nom_table_mod},
                        '3': {'choix': "pour faire une recherche sur la table des variables.",
                              'nom_table': self.nom_table_var},
                        '4': {'choix': "pour faire une recherche sur le dictionnaire des tables.",
                              'nom_table': self.nom_table_dict}}
        print("""Liste des dictionnaires disponibles :\n """)
        for dictionnaire, params in dictionnaires.items():
            print(f"""{dictionnaire} : {params['choix']} \n""")
        time.sleep(1) # Pour éviter que les messages se mélangent
        ok=False
        while not ok:
            dictionnaire = input("""Quel dictionnaire voulez-vous ?""")
            if dictionnaire in dictionnaires:
                params = dictionnaires[dictionnaire]
                ok = True
            else:
                print("""“Choisir le doute comme philosophie de vie c’est comme choisir l’immobilité comme mode de transport.” - Yann Martel """)
        if dictionnaire=='1':
            liste_table = self.liste_table()
            print("""Liste des tables disponibles :""")
            m = max([len(u) for u in liste_table])
            fibonacci = [0,1,1]
            for table in liste_table:
                print(f"{table}" + ' '*(m-len(str(table))) + f" | {fibonacci[-3]}")
                fibonacci.append(fibonacci[-2]+fibonacci[-1])
            time.sleep(1) # Pour éviter que les messages se mélangent
            # plt.plot(fibonacci)
            # plt.show()
            ok = False
            while not ok:
                table = input("Sur quelle table voulez-vous travailler ?")
                if table in liste_table:
                    ok = True
                    requete_sql = f"""
                    select distinct
                    {self.nom_table_var}.id_var,
                    {self.nom_table_var}.nom_var,
                    {self.nom_table_var}.joli_nom_var,
                    {self.nom_table_var}.lib_long_var,
                    {self.nom_table_mod}.id_mod,
                    {self.nom_table_mod}.nom_mod,
                    {self.nom_table_mod}.joli_nom_mod,
                    {self.nom_table_mod}.lib_long_mod
                    from {self.nom_schema}.{table}
                    join {self.nom_schema}.{self.nom_table_var} on {table}.id_var = {self.nom_table_var}.id_var
                    join {self.nom_schema}.{self.nom_table_mod} on {table}.id_mod = {self.nom_table_mod}.id_mod
                    order by id_var, id_mod;
                    """
                else:
                    print("""“Il faut choisir, mourir ou mentir.” - Louis-Ferdinand Céline\n Houla, choisir une table suffira dans un premier temps. """)
        else:
            requete_sql = f"""
            select *
            from {self.nom_schema}.{params['nom_table']}
            """
        df_table = pd.read_sql_query(requete_sql, self.con)

        choix_utilisateur = self.demander_choix_binaire("""Souhaitez-vous faire une recherche par mot_clés ?""")
        if choix_utilisateur:
            # print(f"nom des colonnes: {df_table.columns}")
            display(df_table)

            time.sleep(1)
            ok1=False
            while not ok1:
                col = input("""Ecrivez le nom de.s colonne.s sur laquelle.s vous souhaitez faire une recherche par mot-clé.\n(Séparer par une virgule et sans '' (apostrophes).)""")
                colonnes = list(col.split(','))
                if 0 not in [(col in df_table.columns)*1 for col in colonnes]:
                    ok1=True
                    ok2=False
                    while not ok2:
                        mic = input("Quel mot sera la clé ?")
                        if mic:
                            df_table[col] = df_table[col].astype(str)
                            masque = df_table[col].str.contains(str(mic), case=False, na=False)
                            df_table = df_table[masque]
                            if not df_table.empty:
                                with pd.option_context('display.max_rows', None,
                                        'display.max_columns', None,
                                        'display.precision', 3,
                                        ):
                                    display(df_table)
                            else:
                                print("""Mauvais mot-clé, pas de données...""")
                        else:
                            with pd.option_context('display.max_rows', None,
                                    'display.max_columns', None,
                                    'display.precision', 3,
                                    ):
                                display(df_table)
                        ok2 = True
                else:
                    print("""“Gouverner, c'est choisir (les bons noms de colonne).” - Duc de Lévis """)
        else:
            with pd.option_context('display.max_rows', None,
                    'display.max_columns', None,
                    'display.precision', 3,
                    ):
                display(df_table)

    ##################################################################################################################################
    ### get nom des colonnes
    ##################################################################################################################################

    def get_nom_col(self): #changement table,

        tab_val = self.theme+'_'+self.base+'_'+self.source
        tab_mod = '_{}_mod'.format(self.nom_schema).lower()

        try:
            cursor = self.con.cursor()

            cursor.execute(f"SELECT DISTINCT {tab_mod}.joli_nom_mod, {tab_mod}.id_mod, {tab_mod}.lib_long_mod FROM {self.nom_schema}.{tab_mod},  {self.nom_schema}.{tab_val} WHERE {tab_mod}.id_mod= {tab_val}.id_mod ")  # SELECT {table}.id_mod, {table}.joli_nom_mod, {table}.lib_long_mod FROM {table};
            cursor.close()
            
            tab = pd.read_sql_query(f"SELECT DISTINCT {tab_mod}.joli_nom_mod, {tab_mod}.id_mod, {tab_mod}.lib_long_mod FROM {self.nom_schema}.{tab_mod},  {self.nom_schema}.{tab_val} WHERE {tab_mod}.id_mod= {tab_val}.id_mod ", self.con, index_col=None)
            # display(tab)

            # tri par ordre alphabétique
            tab.sort_values(by=['joli_nom_mod', 'lib_long_mod'], inplace=True)

            joli_nom_mod_l = tab['joli_nom_mod'].tolist()
            lib_long_mod_l = tab['lib_long_mod'].tolist()
            long = 40 # long est la distance entre chaque mot pour chacune des lignes

            print("\n Liste des joli_nom_mod :\n")
            print(joli_nom_mod_l)
            # self.afficher_liste_par_ligne(joli_nom_mod_l, n=5, interval=long)  # n représente le nombre de mots qu'on souhaiterais afficher par ligne

            print("\n")

            print("\n Liste des lib_long_mod :\n")
            print(lib_long_mod_l)
            # self.afficher_liste_par_ligne(lib_long_mod_l, n=5, interval=long) 

            print('\n')

            return tab 

        except (Exception, psycopg2.Error) as error:
            print("Erreur lors de la récupération des noms de colonnes : ", error)
            return pd.DataFrame()


    ##################################################################################################################################
    ### style affichage print()
    ##################################################################################################################################


    def styleP(self, text: str, color: Optional[str] = None, style: Optional[str] = None) -> str:         #(text, color=None, style=None):
        """

        """
        
        # Codes ANSI pour la couleur du texte
        color_codes = {
            'black': '\033[0;30m',
            'red': '\033[0;31m',
            'green': '\033[0;32m',
            'yellow': '\033[0;33m',
            'blue': '\033[0;34m',
            'purple': '\033[0;35m',
            'cyan': '\033[0;36m',
            'white': '\033[0;37m',
            'brown': "\033[0;33m",
            'light_green': "\033[1;32m",
            'light_red': "\033[1;31m",
            'light_blue': "\033[1;34m",
            'reset': '\033[0m'
        }

        # Codes ANSI pour le style du texte
        style_codes = {
            'bold': '\033[1m',
            'italic': '\033[3m',
            'underline': '\033[4m',
            'reset': '\033[0m'
        }

        styled_text = ""

        # Ajoute le code ANSI de style
        if style:
            styled_text += style_codes.get(style, '')

        # Ajoute le code ANSI de couleur
        if color:
            styled_text += color_codes.get(color, '')

        # Ajoute le texte
        styled_text += text

        # Réinitialise le style et la couleur à la fin du texte
        styled_text += style_codes.get('reset', '') + color_codes.get('reset', '')

        return styled_text

    ########################################################################################################

    def jolie_print(self,texte):
        print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+')
        print(self.styleP(texte, color='green'))
        print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+')



    ##############################################################################################################################
    ############# changement à effectuer dans fichier .xlsx
    ##############################################################################################################################

    def modiFichier(self):
        
        """
        Résultat : Affiche à l'utilisateur les modifications qu'il a à apporter au fichier à traiter
        """

        nouvelles_donnees = {}


        df = pd.read_excel(self.nom_excel, sheet_name=None, engine = 'openpyxl')
    
        for sheet, dataframe in df.items():
        # supprimer les caractères spéciaux et les espaces dans les noms de colonnes
            # dataframe.columns = dataframe.columns.str.replace(r'[\(\)\+\-\>\<\?\.]', '', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace(' ', '', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace('+', '_plus_', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace('-', '_moins_', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace('<', '_inf_', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace('>', '_sup_', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace('=', '_egal_', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace(':', '_', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace(')', '', regex=True).str.strip()
            dataframe.columns = dataframe.columns.str.replace('()', '', regex=True).str.strip()
        
    
            for col in dataframe.columns:
                dataframe[col] = dataframe[col].apply(lambda x: '' if isinstance(x, str) and x.isalpha() else x)
    
            nouvelles_donnees[sheet] = dataframe
        
            nouveau_nom_excel = self.nom_excel
    
            with pd.ExcelWriter(nouveau_nom_excel, engine='openpyxl') as writer:
                for sheet_name, data in nouvelles_donnees.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)


        if(len(df)==1):
            print("Plus aucun caractère inattendu dans le nom des colonnes de votre jeu de données \n")
        else:
            print("Plus aucun caractère innnatendu dans le nom des colonnes de vos {} sheets d'intérêt \n".format(len(df)))    

        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')        
        print(self.styleP('Nouveau fichier Excel enregistré avec les modifications à apporter au versement\n' , color= "green") )  #style= 'bold',
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')   


    ##############################################################################################################################
    ############# Remplacer space modalités
    ##############################################################################################################################

    def replacingSpaceModa(listeCols): # listeCols est la liste des modalités
        
        #transformer les charactères en unicode

        ModasinAcc = []
        for modalite in range(len(listeCols)):
            new_string = listeCols[modalite].replace(listeCols[modalite],unidecode.unidecode(listeCols[modalite])).title()
            ModasinAcc.append(new_string)
        # removing commas and points
        for modalite in range(len(ModasinAcc)):
            if((',') in ModasinAcc[modalite]):
                ModasinAcc[modalite]= ModasinAcc[modalite].replace(',','')

            if(('.') in ModasinAcc[modalite]):
                ModasinAcc[modalite]= ModasinAcc[modalite].replace('.','')
                
            if(('(') in ModasinAcc[modalite]):
                ModasinAcc[modalite]= ModasinAcc[modalite].replace('(','')
                
            if((')') in ModasinAcc[modalite]):
                ModasinAcc[modalite]= ModasinAcc[modalite].replace(')','')

            if(('/') in ModasinAcc[modalite]):
                ModasinAcc[modalite]= ModasinAcc[modalite].replace('/','_')

            if((' ') in ModasinAcc[modalite]):
                firstStr = ModasinAcc[modalite][0]
                lastStr = ModasinAcc[modalite][-1]
                if( firstStr==' '):
                    firstStr = firstStr.replace(' ','',1)
                if(lastStr==' '):
                    lastStr = ''.join(lastStr.rsplit(' ', 1))

                middle = ModasinAcc[modalite][1:-1].replace(' ','_')

                ModasinAcc[modalite] = firstStr + middle + lastStr
            
            if(("'") in ModasinAcc[modalite]):
                ModasinAcc[modalite]= ModasinAcc[modalite].replace("'",'_')
                
        return ModasinAcc

    ##############################################################################################################################
    ############# Checking input
    ##############################################################################################################################

    def replacingSpaceInput(entree):

        """
        Donnée   : 

        Résultat :

        """

        # enlever les accents
        correct_entree = unidecode.unidecode(entree).title()

        # sup les caractères spéciaux
        correct_entree = correct_entree.replace(',', '').replace('.', '').replace('(', '').replace(')', '').replace('/', '_').replace("'", '_').replace('+', '_').replace('-','_').replace('*','_').replace('/', '_')

        correct_entree = correct_entree.replace(' ', '_')

        return correct_entree.lower()
    

    ##############################################################################################################################
    ############# Création des vues avec et/ou sans indicateurs
    ##############################################################################################################################

    def creationVueSchema(self): 

        """
            Donnée : self.nom_schema représente le nom du schéma dans lequel le jeu de donées sera versé 

            Résultat : création de deux (O2) vues:
                        * La première est la vue de reconstruction selon la structure de la table du jeu de données à verser
                        * La seconde est une réplique de la prmière vue en plus d'indicateurs à créer comme on le souhaite       
        """

        ############ CREATION DE LA VUE de reconstruction de la table (obligatoire)
        print(self.styleP("""
    Création des vues...""", color='blue', style='bold'))

        ######## STEP 1 ::: RECUPERATION DES TABLES PRINCIPALES DANS LE SCHEMA ####################################################################################
    #  rep = []
        cursor = self.con.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = %s ",(self.nom_schema, self.theme + '_' + self.base + '_' + self.source))
        rep = cursor.fetchall()

        # Affichage des résultats
        if rep:
            print(self.styleP("""
    Table trouvée :\n """, color= 'green'))
            for row in rep:
                print(row[0])  
        else:
            print(self.styleP("""
    Aucune table trouvée.""", color= 'red'))

            ######## STEP 2 ::: RECUPERATION DES self.nom_table_mod A REPRESENTER PAR TABLE ####################################################################################

        for tab in range(len(rep)):
            view_name = rep[tab][0]
            # print(view_name)
            name_avec_schema = self.nom_schema+"."+view_name

            table_name=   name_avec_schema
            # print(table_name)

            # les modalités présentes dans la table
            sqlQuery_modalites = f"select distinct {self.nom_schema}.{self.nom_table_mod}.nom_mod "+f"from {self.nom_schema}.{self.nom_table_mod}, {name_avec_schema} "+f"where {self.nom_table_mod}.id_mod = {name_avec_schema}.id_mod"

            try:
                tab_modalites_avec_type = []
                tab_modalites_sans_type = []
                cursor = self.con.cursor()
                cursor.execute(sqlQuery_modalites)
                result = cursor.fetchall()
                for i in range(len(result)):
                    # ajout du suffix "numeric" lorsque le nom de la modalité commence par '_'
                    new_name = result[i][0]
                    string1 = new_name[0]
                    if(string1 == '_'):
                        new_name = new_name.replace(string1, 'modal'+string1,1)

                    tab_modalites_sans_type.append(str(new_name))
                    tab_modalites_avec_type.append(str('"'+new_name+'"' + " double precision"))  ### changer ici si on veut numeric... plutôt que double_precision

            except (Exception, psycopg2.Error) as error :
                print(sqlQuery_modalites)

            ######## STEP 3 ::: CREATION DE VUES ####################################################################################
            req_echelle = f"select distinct echelle from {name_avec_schema}   ;"
            req_echelle = req_echelle.format(name_avec_schema=name_avec_schema)

            # print(req_echelle)
            try:

                cursor2 = self.con.cursor()
                cursor2.execute(req_echelle)
                result2 = cursor2.fetchall()

                for row in tqdm(result2):
                    self.echelle = row[0]

                    print(self.styleP("""
    Construction de la vue sans indicateur pour l'échelle {}... \n""", color= 'purple').format(self.echelle))

                    view_name_avec_echelle = "{}_{}".format(view_name, self.echelle)

                    sqlQuery =  f"""
                    CREATE VIEW  {self.nom_schema}.V_{view_name_avec_echelle} AS SELECT row_id, code_entite as code_{self.echelle} , annee           
                    """ 
                    for modalite in tab_modalites_sans_type:
                            sqlQuery = sqlQuery+ ", MAX({}) AS {}".format(modalite,modalite)

                    sqlQuery = sqlQuery + f"""    
                        FROM crosstab(
                            ' SELECT concat({name_avec_schema}.code_entite, {name_avec_schema}.annee) as row_id, {name_avec_schema}.code_entite, 
                            {name_avec_schema}.annee, {self.nom_schema}.{self.nom_table_mod}.nom_mod, {name_avec_schema}.valeur
                        FROM  {name_avec_schema}, {self.nom_schema}.{self.nom_table_mod}
                            WHERE {self.nom_schema}.{self.nom_table_mod}.id_mod = {name_avec_schema}.id_mod',
                            $$ values """

                    # Ajout des valeurs précises
                    for i in range(len(tab_modalites_sans_type)):
                        if(i == len(result)-1):                        
                            sqlQuery = sqlQuery+ " ('{}') $$".format(tab_modalites_sans_type[i])
                        else:
                            sqlQuery = sqlQuery+ " ('{}'),".format(tab_modalites_sans_type[i])

                    sqlQuery = sqlQuery + ') AS table_vue ("row_id" text,"code_entite" text, "annee" text ,'    #  integer

                    for i in range(len(tab_modalites_avec_type)):
                        if(i == len(result)-1):
                            sqlQuery = sqlQuery+ " {}".format(tab_modalites_avec_type[i])
                        else:
                            sqlQuery = sqlQuery+ " {},".format(tab_modalites_avec_type[i])
                    sqlQuery = sqlQuery + ") GROUP BY row_id, code_entite, annee ORDER BY code_entite ASC;"
                    
                    # print(sqlQuery)
                    dropingLine = 'DROP VIEW  IF EXISTS ' + self.nom_schema+'.V_'+view_name_avec_echelle + ' CASCADE;'
                    cursor2.execute(dropingLine)
                    cursor2.execute(sqlQuery)
                    self.con.commit()

                    print('_'*100 , '\n')
                    print(self.styleP("""
    La vue """ + self.nom_schema+'.V_'+view_name_avec_echelle + " a été créée avec succès dans PostgreSQL \n", style='bold'))
                

            except (Exception, psycopg2.Error) as error :   
                print(error)


        ########################### calcul des indicateurs        

        view_name = rep[tab][0]
        name_avec_schema = self.nom_schema+"."+view_name
        table_name=   name_avec_schema


        OK = False
        while( not  OK):
            reponse = input(self.styleP(""" 
    Souhaitez-vous calculer des indicateurs à partir de la vue créée? \n
    Veuillez répondre par OUI ou NON \n """ , style= 'bold', color= 'blue')).upper()

            if(reponse == ""):
                print(""" 
    Veuillez répondre à la question par OUI ou NON \n """) 
                OK = False               

            elif(reponse == 'NON'):
                print(self.styleP("""
    Opérations terminées  """, color='green'))
                OK = True

            ########## SI INDICATEURS à CREER

            elif(reponse == 'OUI'):

                print(self.styleP("""
    Vous pouvez ajouter des indicateurs calculés pour plus d'informations sur vos données \n """, style= 'bold'))
                    

                OK = True

                choix_ok=False
                while(not choix_ok):
                    ### liste des colonnes
                                    
                    table_mod = self.nom_schema+'.'+'_{}_mod'.format(self.nom_schema).lower()  #######   ATTENTION  #'.V_'+view_name

                    info_col = self.get_nom_col()  #table_mod,

                    if info_col.empty:    #  not 
                        print("Aucune colonne trouvée.")
                        self.con.close()               

                    choix_ok=True

                description = ""
                while description.strip() == "":
                    description = input("""
    Avant de commencer veuillez donner une description pour la vue avec indicateur à créer : \n
    (Vous pourrez vous baser sur les indicateurs que vous allez créer ...)
    """)
                    if description.strip() == "":
                        print(self.styleP("""
    Nous vous conseillons de donner une description pour votre vue avec indicateur !\n""", color='red'))    

                nb_ind = None #int(input("\n Entrez le nombre d'indicateurs à calculer : "))

                ### ajout boucle while pour exceptions
                while nb_ind is None:
                    try:
                        saisie = input("""
    Entrez le nombre d'indicateurs à calculer : """)
                        if saisie.strip() == "":
                            print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red')) 
                        elif not saisie.isdigit():
                            print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red'))
                        elif saisie == '0':
                            print(self.styleP("""
    Veuillez entrer un nombre à partir de 1  """, color='red')) 
                        else:
                            nb_ind = int(saisie)
                        # nb_ind = int(saisie)

                    except ValueError as e:
                        print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red'))  

                infos_ind = []

                print('\n')
                print(self.styleP("1:", style="bold"), " Part ... en % (modalite1/modalite2) * 100  ou [ (modalite1 + modalite2 + .... + modaliteN-1)/modaliteN ] * 100 \n")                                
                print(self.styleP("2:", style="bold"), " Somme (modalite1 + modalite2 + .... + modaliteN) \n" )                          
                print(self.styleP("3:", style="bold"), " Différence (modalite1 - modalite2 - .... - modaliteN)\n") 
                print(self.styleP("4:", style="bold"), " Division (modalite1/modalite2) ou [ (modalite1 + modalite2 + .... + modaliteN-1)/modaliteN ]  \n")    

                for i in range(nb_ind):
                    print(f"""
    Indicateur {i+1}: """)   

                    print("""
    Quel type de calcul souhaitez-vous effectuer? \n""")
                    print(self.styleP("""
    Veuillez entrez un numéro parmis les choix qui vous sont proposés ci-dessus: \n""", style="bold"))                       
                                                                
                    type_calcul = None  ############ input() int!         

                    # ajout condition while pour exceptions
                    while type_calcul not in ['1', '2', '3', '4']:
                        type_calcul = input()
                        if type_calcul.strip() not in ['1', '2', '3', '4']:
                            print(self.styleP("""
    Veuillez saisir une des valeurs mentionnées  """, color='red'))
                            type_calcul = None

                    info_col = self.get_nom_col()
                    liste_libelles = [ lib.lower() for lib in info_col['joli_nom_mod'].tolist() ]
                    time.sleep(1)
                    while True:
                        saisie_colonnes = input("""
    Veuillez saisir les modalités pour le calcul (séparées pas des virgules) : """)                
                        
                        if re.match(r'^\w+(,\s*\w+)*$', saisie_colonnes):
                            colonnes = [colonne.strip().lower() for colonne in saisie_colonnes.split(',')]

                            ok = True
                            for col in colonnes:
                                if col not in liste_libelles:
                                    
                                    print(self.styleP("""
    La colonne {} n'existe pas ou a été mal renseignée""", color = 'red').format(col))
                                    ok = False
                            if ok:
                                break

                        elif saisie_colonnes == "":
                            print(self.styleP("""
    La saisie ne peut être vide ! """, color='red'))
                            
                        else:
                            print(self.styleP("""
    Les modalités doivent être séparées par une virgule ! """, color='red'))

                    nom_ind = "" #input(""" Choisissez un nom pour l\'indicateur à créer :  \n""" )  

                    # ajout boucle while pour gérer les exceptions
                    while nom_ind.strip() == "":
                        nom_ind = input(""" 
    Choisissez un nom pour l\'indicateur à créer :  """ )
                        if nom_ind.strip() == "":
                            print("""
    Le nom de l'indicateur ne peut pas être vide.""")
                            
                    #### Création de l'indicateur
                    
                    ## dictionnaire des modalités avec leur type pour la requête
                    info_ind = {"type": type_calcul, "variables": colonnes, "nom_indicateur": nom_ind}
                    infos_ind.append(info_ind)


                    ## requête des vues avec indicateur

                    req_echelle = f"select distinct echelle from {name_avec_schema}   ;"
                    req_echelle = req_echelle.format(name_avec_schema=name_avec_schema)

                    # print(req_echelle)
                    try:
                        cursor2 = self.con.cursor()
                        cursor2.execute(req_echelle)
                        result2 = cursor2.fetchall()

                        for row in result2:
                            self.echelle = row[0]

                            print(self.styleP("""
    Construction de la vue avec indicateur pour l'échelle {}... \n""", color= 'purple').format(self.echelle))

                            view_name_avec_echelle = "{}_{}".format(view_name, self.echelle)


                            sqlQuery2 = f""" 
                            CREATE VIEW  {self.nom_schema}.I_{view_name_avec_echelle}  AS SELECT row_id, code_{self.echelle}, annee, """

                            for info_ind in infos_ind:

                                type_calcul = info_ind["type"]

                                variables = info_ind["variables"]
                        
                                nom_indicateur = info_ind["nom_indicateur"]

                                ##### définition des opérateurs par type de calcul

                                colonnesPlus = "+".join(info_ind["variables"])
                                colonnesMoins= "-".join(info_ind["variables"])
                                colonnesPart = "/".join(f"NULLIF({col}, 0)" for col in variables) 

                                nom_ind = info_ind["nom_indicateur"]
                            #    print(colonnes)
                            #    print(colonnes[0])

                                if type_calcul == '2':
                                    #  sqlQuery2 += f"coalesce({colonnesPlus},0) AS {nom_ind}, " 
                                    sqlQuery2 +=   f"CASE WHEN NOT ({colonnesPlus} IS NULL)  OR {colonnesPlus} != 0 THEN {colonnesPlus} ELSE NULL END AS {nom_ind}, "

                                elif type_calcul == '3':
                                    #  sqlQuery2 += f"coalesce({colonnesMoins},0)  AS {nom_ind}, "
                                    sqlQuery2 +=   f"CASE WHEN NOT ({colonnesMoins} IS NULL) OR {colonnesMoins} != 0 THEN {colonnesMoins} ELSE NULL END AS {nom_ind}, "
                        
                                elif type_calcul == '1':
                                    somme_num_col = "+".join(variables[:-1])
                                    #somme_num_col = "("+somme_num_col+")"
                                    denum_col = variables[-1]


                                    sqlQuery2 += f"CASE WHEN NOT ({somme_num_col} IS NULL) AND {denum_col} IS NOT NULL AND {denum_col} != 0 THEN (( ({somme_num_col}) / {denum_col}) * 100) ELSE NULL END AS {nom_ind}, "    

                                elif type_calcul == '4':
                                    somme_num_col = "+".join(variables[:-1])
                                    denum_col = variables[-1]

                                    sqlQuery2 += f"CASE WHEN NOT ({somme_num_col} IS NULL) AND {denum_col} IS NOT NULL AND {denum_col} != 0 THEN (( ({somme_num_col}) / {denum_col}) ) ELSE NULL END AS {nom_ind}, "   

                                
                            sqlQuery2= sqlQuery2[:-2]  # pour enlever la dernière virgule avant le FROM

                            sqlQuery2 += ' FROM  ' + self.nom_schema+'.V_'+view_name_avec_echelle + ' ORDER BY row_id ASC; ' 

                            sqlQuery2 = sqlQuery2.format(view_name_avec_echelle=view_name_avec_echelle)    

                            # print(sqlQuery2) 
        
                # try:
                            # cur = self.con.cursor()
                            dropingLine = 'DROP VIEW  IF EXISTS ' + self.nom_schema+'.I_'+view_name_avec_echelle + ';'
                            cursor2.execute(dropingLine)
                            cursor2.execute(sqlQuery2)
                            self.con.commit()

                            cursor2.execute(f"""
                            INSERT INTO {self.nom_schema}.indicateurs_descriptions (nom_indicateur, description)
                            VALUES (%s, %s)
                            ON CONFLICT (nom_indicateur) DO UPDATE SET description = EXCLUDED.description;  
                            """, (self.nom_schema+'.I_'+view_name_avec_echelle, description))
                            self.con.commit()

                            print('+-'*80, '\n')
                            print(self.styleP("La vue inidcateur " + self.nom_schema+'.I_'+view_name_avec_echelle + " a été créée avec succès dans PostgreSQL \n", style= 'bold', color= 'cyan'))
                            print('+-'*80, '\n')

                    except (Exception) as error :
                        print ("Erreur lors de la création de la vue indicateur"+ self.nom_schema+'.I_'+view_name_avec_echelle +" dans PostgreSQL: ", error)
                        print(sqlQuery2)

            else:
                print(""" 
    Veuillez répondre à la question par OUI ou NON \n """)
                OK = False


    ##############################################################################################################################
    ############# Informations sur les indicateurs créés
    ##############################################################################################################################

    def description_indicateur(self):

        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
    
        cur = self.con.cursor()

        req = 'SELECT nom_indicateur, description FROM '  + self.nom_schema+'.indicateurs_descriptions '

        df = pd.read_sql_query(req, self.con)
        display(df)  


    ##############################################################################################################################
    ############# Liste des colonnes d'une table sur PgAdmin  
    ##############################################################################################################################     


    def list_col_table(self, nom_table):
        cur = self.con.cursor()

        req = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{self.nom_schema}'
        AND table_name = '{nom_table}'
        AND column_name NOT IN ('row_id', 'annee')
        ORDER BY column_name ASC;
        """
        cur.execute(req)
        columns = cur.fetchall()
        return [col[0] for col in columns]  
    

    ##############################################################################################################################
    ############# Affichage lignes par lignes de différentes listes
    ##############################################################################################################################


    def afficher_liste_par_ligne(self, liste, n, interval):

        long = 40
        for i, valeur in enumerate(liste, 1):
            print(valeur.ljust(long), end='')
            if i % n == 0:
                print()


    ##############################################################################################################################
    ############# Création de vue par année
    ##############################################################################################################################

    def create_vue_par_annee(self):

        """
            Donnée : 

            Résultat : 

        """

        global choix_nom

        cur = self.con.cursor()


        # demande si sûr de modifier la définition de la vue

        OK = False
        time.sleep(1)
        while( not  OK):
            reponse = input(self.styleP(""" 
    Etes-vous sûr(e) de vouloir créer une vue pour une année spécifique ? \n
    Veuillez répondre par OUI ou NON \n
    """ , style= 'bold', color= 'blue')).upper()

            if(reponse == ""):
                print(""" 
    Veuillez répondre à la question par OUI ou NON \n """) 
                OK = False               

            elif(reponse == 'NON'):
                print(self.styleP("""
    Opérations terminées  """, color='green'))
                OK = True

            elif(reponse == 'OUI'):

                # saisie de la vue (liste des vues existantes dans le schéma)

                cur.execute("""
                SELECT table_name as vue
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name like %s
                AND table_type = 'VIEW'
                UNION 
                SELECT DISTINCT dependent_view.relname as vue 
                FROM pg_depend 
                JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid 
                JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid 
                JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid 
                JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid 
                AND pg_depend.refobjsubid = pg_attribute.attnum 
                JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
                JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
                WHERE 
                source_ns.nspname = %s
                AND source_table.relname like %s;
                """, (self.nom_schema,'%{}_{}_{}%'.format(self.theme, self.base, self.source),self.nom_schema,'%{}_{}_{}%'.format(self.theme, self.base, self.source),))

                liste_tab = cur.fetchall()
                if liste_tab:
                    print(self.styleP("""
    Vues trouvées :\n""", color= 'green', style= 'bold'))
                    for row in liste_tab:
                        print("""
    """,row[0])  
                else:
                    print(self.styleP("""
    Aucune vue trouvée.""", color= 'red'))
                    
                nom_vue = "" 

                # ajout boucle while pour gérer les exceptions
                time.sleep(1)
                while nom_vue.strip() == "":
                    nom_vue = input(""" 
    Veuillez saisir le nom complet d'une des vues mentionnées plus haut :  \n
    """ )
                    if nom_vue.strip() == "":
                        print(self.styleP(""""
    Le nom de l'indicateur ne peut pas être vide.""", color='red'))
                        
                print('\n')

                description = ""
                time.sleep(1)
                while description.strip() == "":
                    description = input("""
    Avant de commencer veuillez donner une description pour la vue avec indicateur à créer : \n
    (Vous pourrez vous baser sur les indicateurs que vous allez créer ...)
    """)
                    if description.strip() == "":
                        print(self.styleP("""
    Nous vous conseillons de donner une description pour votre vue avec indicateur !""", color='red'))        
                
                # Liste des années présentes
                name_avec_schema = self.nom_schema+"."+self.theme+"_"+self.base+"_"+self.source
                req_annee = f"select distinct annee from {name_avec_schema}   ;"
                req_annee = req_annee.format(name_avec_schema=name_avec_schema)

                try:
                    cur.execute(req_annee)
                    res = cur.fetchall()
                
                    annee = [row[0] for row in res]

                    print('\n')
                    for val in annee:
                        print("""
    """,val)


                except (Exception, psycopg2.Error) as error :   
                    print(error)    
                    
            # Choix de l'année
                choix_annee = ""  
                time.sleep(1)
                # ajout boucle while pour gérer les exceptions
                while choix_annee.strip() == "" or int(choix_annee) not in annee:
                    choix_annee = input(""" 
    Veuillez choisir une année parmi celles référencées plus haut :  \n
    """ )
                    if choix_annee.strip() == "":
                        print(self.styleP("""
    L'entrée ne peut être vide.""", color='red'))
                        
                choix_nom2 = ""
                time.sleep(1)
                while choix_nom2.strip() == "" :
                    choix_nom2 = input(""" 
    Veuillez saisir un nom pour la nouvelle vue indicateur qui va être créée :  \n
    """ )
                    if choix_nom2.strip() == "":
                        print(self.styleP("""
    L'entrée ne peut être vide.""", color='red'))        
                        
                        
            # Création de la vue
                cur.execute(""" SELECT definition
            FROM pg_views
            WHERE viewname = %s
            AND schemaname = %s ;
            """,(nom_vue,self.nom_schema))
                
                req1 = cur.fetchone()
                
                if req1 is None:
                    raise ValueError(f"La vue {self.nom_schema}.{nom_vue} n'existe pas.")
                
                # définition de la vue
                req1 = req1[0]
                # print(req1)

                if 't1.' in req1.replace(' ', ''):
                    # Si 't1' est présent, ne rien changer
                    req2 = req1                   
                else:
                #     # Si 't1' n'est pas présent remplacer
                #     req2 = req1.replace('t1', 'table_vue')
                    req2 = req1

                if "GROUP BY" in req2:
                    parts = req2.split("GROUP BY")
                    if "ORDER BY" in parts[1]:
                        sub_parts = parts[1].split("ORDER BY")
                        req2 = f"{parts[0]} WHERE t1.annee = '{choix_annee}' GROUP BY {sub_parts[0]} ORDER BY {sub_parts[1]}"
                    else:
                        req2 = f"{parts[0]} WHERE t1.annee = '{choix_annee}' GROUP BY {parts[1]}"
                elif "ORDER BY" in req1:
                    parts = req2.split("ORDER BY")
                    req2 = f"{parts[0]} WHERE t1.annee = '{choix_annee}' ORDER BY {parts[1]}"
                else:
                    req2 = req2 + f" WHERE t1.annee = '{choix_annee}'"


                if 't1' in req1 :  # and 't1' not in req1

                    req2 = req2.replace('WHERE t1.annee', 'WHERE t1.annee')

                elif 'table_vue' in req1 :

                    req2 = req2.replace('WHERE t1.annee', 'WHERE table_vue.annee') 

                else:
                    if "ORDER BY" in req1:
                        parts = req2.split("ORDER BY")
                        match = re.search(r"(\w+)\.\w+", parts[1])
                        if match:
                            table_name = match.group(1)
                            req2 = req2.replace('WHERE t1.annee', f'WHERE {table_name}.annee')   

                
                # modif
                req2 = f"CREATE OR REPLACE VIEW {self.nom_schema}.I_{choix_nom2} as" + req2     #.replace(f"CREATE OR REPLACE VIEW {self.nom_schema}.{nom_vue}",  f"CREATE OR REPLACE VIEW {self.nom_schema}.{choix_nom2}")
                
                # print(req2)
                
                # Création de la nouvelle vue
                cur.execute(req2)
                
                # Validation des modifications dans la self.base de données
                self.con.commit()


                cur.execute(f"""
                INSERT INTO {self.nom_schema}.indicateurs_descriptions (nom_indicateur, description)
                VALUES (%s, %s)
                ON CONFLICT (nom_indicateur) DO UPDATE SET description = EXCLUDED.description;  
                """, (self.nom_schema+'.I_'+choix_nom2, description))
                self.con.commit()


                print('+-'*80, '\n')
                print(self.styleP("La nouvelle vue indicateur" + self.nom_schema+'.I_'+choix_nom2  + " a été créée avec succès dans PostgreSQL \n", style= 'bold', color= 'cyan'))
                print('+-'*80, '\n')

                
                OK = True        
                

            else:
                print(""" 
    Veuillez répondre à la question par OUI ou NON \n """)
                OK = False


    ##############################################################################################################################
    ############# Création nouvelle vue à partir d'une vue existante
    ##############################################################################################################################


    def create_new_vue(self):  #self.base,self.source,self.theme

        """
            Donnée : 

            Résultat : 

        """

        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        
    # Méthode 1 : Définition des variables d'environnement
    # Activer le mode unbuffered pour la sortie standard
        os.environ['PYTHONUNBUFFERED'] = '1'

        global choix_nom

        cur = self.con.cursor()

        print(self.styleP("""
    Que souhaitez vous faire ?""", style= 'bold', color= 'purple'))
        print(self.styleP("""
    1: Créer une nouvelle vue avec indicateurs sans jointure \n
    2: Créer une vue avec indicateurs par jointure à une autre vue \n                  
    """, style= 'bold'))
        time.sleep(1)
        while True:
            choix_vue = input()

            if choix_vue == '1':

                cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name NOT LIKE %s
                AND table_type = 'VIEW' ORDER BY table_name ASC
                """, (self.nom_schema, 'dico%'))

                vues = cur.fetchall()
                if vues:
                    print(self.styleP("""
                    Vues trouvées :\n
                    """, color= 'green', style= 'bold'))
                    for row in vues:
                    #     print("""\t
                    # """,row[0]) 
                        self.afficher_liste_par_ligne(row, n=3, interval=40)

                else:
                    print(self.styleP("""
    Aucune vue trouvée.""", color= 'red'))
                print('\n')
                
                print(self.styleP(""" 
    Vous êtes sûr(e) de vouloir créer une nouvelle vue ? \n
    Répondez par oui ou non! \n """ , style= 'bold', color= 'blue'))
                time.sleep(1)
                OK = False
                while( not  OK):
                    reponse = input().upper()

                    if(reponse == ""):
                        print(""" 
    Veuillez répondre à la question par OUI ou NON \n """)               

                    elif(reponse == 'NON'):
                        print(self.styleP("""
    Opérations terminées """, color='green'))
                        OK = True

                    ########## SI INDICATEURS à ajouter
                    elif(reponse == 'OUI'):

                        print(self.styleP("""
    Vous pouvez ajouter des indicateurs calculés pour avoir plus d'informations sur vos données \n """, style= 'bold'))
                        
                        nom_vue = input("""
    Veuillez saisir le nom complet d'une des vues affichées précédement: \n
    """ )
                        OK = True

                        choix_ok=False
                        while(not choix_ok):
                            ### liste des colonnes
                                            
                            table_mod = self.nom_schema+'.'+'_{}_mod'.format(self.nom_schema).lower()  #######   ATTENTION  #'.V_'+view_name

                            # info_col = get_nom_col( self.con, self.theme, self.base, self.source) #table_mod,

                            print('\n')
                            info_col = self.list_col_table(nom_vue)
                            self.afficher_liste_par_ligne(info_col, n=5, interval=40)

                            print('\n')

                            if not info_col:  # .empty
                                print("Aucune colonne trouvée.")
                                self.con.close()

                            else:
                                choix_ok=True

                        req = ' SELECT * FROM ' + self.nom_schema+'.'+nom_vue+' LIMIT 5; '
                        # print(req)      

                        try:
                            
                            cur.execute(req)
                            self.con.commit()

                            sol = cur.fetchall()
                            #   revoir affichage
                            print(self.styleP("""
    Voici un apercu de la structure de la table sur PgAdmin """, style= 'bold'))
                            
                            df = pd.read_sql_query(req, self.con)
                            display(df)

                        except (Exception) as error :
                            print (error)


                        ####### création de la nouvelle vue
                        
                        choix_nom = input("""
    Entrez un nom pour la vue qui va être créée \n
    Petit conseil ! Vous pouvez reprendre le nom de votre vue sans le 'V' et y ajouter un 2 (Exemple : jeunesse_college_dept34_commune2 )  \n""")
                        
                        description = ""
                        while description.strip() == "":
                            description = input("""
    Avant de commencer veuillez donner une description pour la vue avec indicateur à créer : \n
    (Vous pourrez vous baser sur les indicateurs que vous allez créer ...)
    """)
                            if description.strip() == "":
                                print(self.styleP("""
    Nous vous conseillons de donner une description pour votre vue avec indicateur !""", color='red'))
                                
                        nb_ind = None #int(input("\n Entrez le nombre d'indicateurs à calculer : "))

                        ### ajout boucle while pour exceptions
                        while nb_ind is None:
                            try:
                                saisie = input("""
    Entrez le nombre d'indicateurs à calculer : 
    """)
                                if saisie.strip() == "":
                                    print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red')) 
                                elif not saisie.isdigit():
                                    print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red'))
                                elif saisie == '0':
                                    print(self.styleP("""
    Veuillez entrer un nombre à partir de 1  """, color='red')) 
                                else:
                                    nb_ind = int(saisie)

                                # nb_ind = int(saisie)

                            except ValueError as e:
                                print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red'))  

                        infos_ind = []

                        print('\n')
                        print(self.styleP("1:", style="bold"), " Part ... en % (modalite1/modalite2) * 100  ou [ (modalite1 + modalite2 + .... + modaliteN-1)/modaliteN ] * 100 \n")                                
                        print(self.styleP("2:", style="bold"), " Somme (modalite1 + modalite2 + .... + modaliteN) \n" )                          
                        print(self.styleP("3:", style="bold"), " Différence (modalite1 - modalite2 - .... - modaliteN) \n") 
                        print(self.styleP("4:", style="bold"), " Division (modalite1/modalite2) ou [ (modalite1 + modalite2 + .... + modaliteN-1)/modaliteN ] \n")    

                        for i in range(nb_ind):
                            print(f"""
    Indicateur {i+1}: """)   

                            print("""
    Quel type de calcul souhaitez-vous effectuer? \n""")
                            print(self.styleP("""
    Veuillez entrez un numéro parmis les choix qui vous sont proposés ci-dessus: \n""", style="bold"))                       
                                                                        
                            type_calcul = None  ############ input() int!         

                            # ajout condition while pour exceptions
                            while type_calcul not in ['1', '2', '3', '4']:
                                type_calcul = input()
                                if type_calcul.strip() not in ['1', '2', '3', '4']:
                                    print(self.styleP("""
    Veuillez saisir une des valeurs mentionnées  """, color='red'))
                                    type_calcul = None


                            info_col = self.list_col_table(nom_vue)
                            time.sleep(1)
                            while True:

                                saisie_colonnes = input("""
    Veuillez saisir les modalités pour le calcul (séparées pas des virgules) : """)
                                if re.match(r'^\w+(,\s*\w+)*$', saisie_colonnes):
                                    colonnes = [colonne.strip().lower() for colonne in saisie_colonnes.split(',')]

                                    ok = True
                                    for col in colonnes:
                                        if col not in info_col:
                                            print(self.styleP("""
    La colonne {} n'existe pas ou a été mal renseignée """, color='red').format(col))
                                            ok = False

                                    if ok:
                                        break

                                else:
                                    print(self.styleP("""
    Les modalités doivent être séparées par une virgule ! """, color='red')) 

                            nom_ind = "" #input(""" Choisissez un nom pour l\'indicateur à créer :  \n""" )  

                            # ajout boucle while pour gérer les exceptions
                            while nom_ind.strip() == "":
                                nom_ind = input(""" 
    Choisissez un nom pour l\'indicateur à créer :  \n
    """ )
                                if nom_ind.strip() == "":
                                    print(self.styleP("""
    Le nom de l'indicateur ne peut pas être vide.""", color= 'red'))
                                    
                            #### Création de l'indicateur
                            
                            ## dictionnaire des modalités avec leur type pour la requête
                            info_ind = {"type": type_calcul, "variables": colonnes, "nom_indicateur": nom_ind}
                            infos_ind.append(info_ind)

                        sqlQuery2 = f""" CREATE VIEW """ +self.nom_schema+'.I_'+choix_nom + """  AS SELECT *, """  # ICI
                        # req = ' SELECT * FROM ' + self.nom_schema+'.'+nom_vue+' LIMIT 5; '
                        for info_ind in infos_ind:

                            type_calcul = info_ind["type"]

                            variables = info_ind["variables"]
                    
                            nom_indicateur = info_ind["nom_indicateur"]

                            ##### définition des opérateurs par type de calcul

                            colonnesPlus = "+".join(info_ind["variables"])
                            colonnesMoins= "-".join(info_ind["variables"])
                            colonnesPart = "/".join(f"NULLIF({col}, 0)" for col in variables) 

                            nom_ind = info_ind["nom_indicateur"]

                            if type_calcul == '2':
                                #  sqlQuery2 += f"coalesce({colonnesPlus},0) AS {nom_ind}, " 
                                sqlQuery2 +=   f"CASE WHEN NOT ({colonnesPlus} IS NULL)  OR {colonnesPlus} != 0 THEN {colonnesPlus} ELSE NULL END AS {nom_ind}, "

                            elif type_calcul == '3':
                                #  sqlQuery2 += f"coalesce({colonnesMoins},0)  AS {nom_ind}, "
                                sqlQuery2 +=   f"CASE WHEN NOT ({colonnesMoins} IS NULL) OR {colonnesMoins} != 0 THEN {colonnesMoins} ELSE NULL END AS {nom_ind}, "
                    
                            elif type_calcul == '1':
                                somme_num_col = "+".join(variables[:-1])
                                #somme_num_col = "("+somme_num_col+")"
                                denum_col = variables[-1]

                                sqlQuery2 += f"CASE WHEN NOT ({somme_num_col} IS NULL) AND {denum_col} IS NOT NULL AND {denum_col} != 0 THEN (( ({somme_num_col}) / {denum_col}) * 100) ELSE NULL END AS {nom_ind}, "    

                            elif type_calcul == '4':
                                somme_num_col = "+".join(variables[:-1])
                                denum_col = variables[-1]

                                sqlQuery2 += f"CASE WHEN NOT ({somme_num_col} IS NULL) AND {denum_col} IS NOT NULL AND {denum_col} != 0 THEN (( ({somme_num_col}) / {denum_col}) ) ELSE NULL END AS {nom_ind}, "   

                            
                        sqlQuery2= sqlQuery2[:-2]

                        sqlQuery2 += ' FROM  ' + self.nom_schema+'.'+nom_vue + ' ; ' 

                        sqlQuery2 = sqlQuery2.format(view_name=nom_vue) 

                        # print(sqlQuery2)
                        

                        try:
                            cur = self.con.cursor()
                            dropingLine = 'DROP VIEW  IF EXISTS ' + self.nom_schema+'.I_'+choix_nom + ';'  #+'_avec_Indicateur'
                            cur.execute(dropingLine)
                            cur.execute(sqlQuery2)
                            self.con.commit()

                            cur.execute(f"""
                            INSERT INTO {self.nom_schema}.indicateurs_descriptions (nom_indicateur, description)
                            VALUES (%s, %s)
                            ON CONFLICT (nom_indicateur) DO UPDATE SET description = EXCLUDED.description;  
                            """, (self.nom_schema+'.I_'+choix_nom, description))
                            self.con.commit()

                            print('+-'*80, '\n')
                            print(self.styleP("La nouvelle vue indicateur" + self.nom_schema+'.I_'+choix_nom  + " a été créée avec succès dans PostgreSQL \n", style= 'bold', color= 'cyan'))
                            print('+-'*80, '\n')

                        except (Exception) as error :
                            print ("Erreur lors de la création de la vue indicateur "+ self.nom_schema+'.I_'+choix_nom  +" dans PostgreSQL: ", error)
                            print(sqlQuery2)

                break


            elif choix_vue == '2': 

                print(self.styleP("""
    Vous êtes sur le point de créer des indicateurs par jointure sur une autre vue \n 
    Voici la liste des vues d'intérêt selon les thème, base et source choisis """, style= 'bold')) 

                cur.execute("""
                SELECT table_name as vue
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'VIEW'
                AND table_name LIKE %s
                AND table_name NOT LIKE %s
                UNION 
                SELECT DISTINCT dependent_view.relname as vue 
                FROM pg_depend 
                JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid 
                JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid 
                JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid 
                JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid 
                AND pg_depend.refobjsubid = pg_attribute.attnum 
                JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
                JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
                WHERE 
                source_ns.nspname = %s
                AND source_table.relname like %s;
                """, (self.nom_schema, '%{}_{}_{}%'.format(self.theme, self.base, self.source), 'dico%',self.nom_schema, '%{}_{}_{}%'.format(self.theme, self.base, self.source)))
                

                liste_tab_interet = cur.fetchall() 


                if liste_tab_interet:
                    print(self.styleP("""
    Vues trouvées :\n""", color= 'green', style= 'bold'))
                    for row in liste_tab_interet:
                        print("""
                """,row[0])  
                else:
                    print(self.styleP("""
    Aucune vue trouvée.""", color= 'red'))         

                print('\n')
                nom = ""
                while nom.strip() == "":
                    nom = input(self.styleP(""" 
    Entrez le nom de la vue d'intérêt sur laquelle vous souhaitez effectuer la jointure \n 
    """, style= 'bold'))
                    if nom.strip() == "":
                        print(self.styleP("""
    Le nom de la vue ne peut pas être vide.""",color='red'))
                        

                print(self.styleP("""
    Voici la liste des vues avec lesquelles vous pourrez effectuer la jointure: \n 
    (Elles sont de ce fait naturellement différentes de votre vue d'interêt !) """, style='bold'))

                cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'VIEW'
                AND table_name != %s
                AND table_name NOT LIKE %s
                ORDER BY table_name ASC;
                """, (self.nom_schema, nom, 'dico%'))     # '{self.nom_schema}'

                liste_tab = cur.fetchall()
                if liste_tab:
                    print(self.styleP("""
    Vues trouvées :\n""", color= 'green', style= 'bold'))
                    for row in liste_tab:
                        print("""
    """,row[0])  
                else:
                    print(self.styleP("""
    Aucune vue trouvée.""", color= 'red'))

                nom2 = ""
                while nom2.strip() == "":
                    nom2 = input(self.styleP(""" 
    Entrez le nom de la vue avec laquelle vous souhaitez faire la jointure \n 
    Attention ! Assurez-vous que la vue sélectionnée aie la même échelle que votre vue de base ! \n 
    """, style= 'bold'))
                    
                    if nom2.strip() == "":
                        print(self.styleP("""
    Le nom de la vue ne peut pas être vide.""",color='red'))

                # nom2 = nom2.split('_')
                # if len(nom2) >= 3:
                #     self.theme2= nom2[1]  
                #     self.base2= nom2[2] 
                #     self.source2 = nom2[3]        

                print(self.styleP("""
    Informations : Liste des modalités concernant la vue d'intérêt {} : \n""", style= 'bold', color= 'purple').format(nom))
                info_col1 = self.list_col_table(nom)   # get_nom_col( self.con, self.theme, self.base, self.source)
                # print('\t',list(info_col1))
                self.afficher_liste_par_ligne(info_col1, n=5, interval=40)

                print('\n')

                print(self.styleP("""
    Informations : Liste des modalités concernant la vue de jointure {} : \n""", style= 'bold', color= 'purple').format(nom2)) # format('_'.join(nom2))
                info_col2 = self.list_col_table(nom2)  # get_nom_col( self.con, self.theme2, self.base2, self.source2)   # ('_'.join(nom2))
                # print('\t',list(info_col2))
                self.afficher_liste_par_ligne(info_col2, n=5, interval=40)

                self.echelle = None
                for col in info_col1:
                    if col.startswith('code_'):
                        self.echelle = col
                        break
                    
                print('\n')
                # DEBUT CONSTRUCTION VUE
                choix_nom2 = ""
                while choix_nom2.strip() == "":
                    choix_nom2 = input("""
    Entrez un nom pour la vue JOINTURE qui va être créée \n
    Petit conseil ! Vous pouvez reprendre le nom de votre vue sans le 'I' et y ajouter un 2 (Exemple : jeunesse_college_dept34_commune2 ) \n
    """)
                    if choix_nom2.strip() == "":
                        print(self.styleP("""
    Le nom de votre vue à créer ne peut pas être vide """, color= 'red'))

                print('\n')

                description = ""
                while description.strip() == "":
                    description = input("""
    Avant de commencer veuillez donner une description pour la vue avec indicateur à créer : \n
    (Vous pourrez vous baser sur les indicateurs que vous allez créer ...)
    """)
                    if description.strip() == "":
                        print(self.styleP("""
    Nous vous conseillons de donner une description pour votre vue avec indicateur !""", color='red'))

                print('\n')

                nb_ind2 = None   # int(input("\n Entrez le nombre d'indicateurs à calculer : "))

            ### ajout boucle while pour exceptions
                while nb_ind2 is None:
                    try:
                        saisie = input("""
    Entrez le nombre d'indicateurs à calculer : """)
                        if saisie.strip() == "":
                            print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red')) 
                        elif not saisie.isdigit():
                            print(self.styleP("""
    Veuillez entrer un nombre valide  """, color='red')) 
                        elif saisie == '0':
                            print(self.styleP("""
    Veuillez entrer un nombre à partir de 1  """, color='red'))
                        else:
                            nb_ind2 = int(saisie)

                    except ValueError as e:
                        print(self.styleP("""
    Votre saisie n'est pas valide  """, color='red'))  

                infos_ind2 = []

                print('\n')
                print(self.styleP("1:", style="bold"), " Part ... en % (modalite1/modalite2) * 100  ou [ (modalite1 + modalite2 + .... + modaliteN-1)/modaliteN ] * 100 \n")                                
                print(self.styleP("2:", style="bold"), " Somme (modalite1 + modalite2 + .... + modaliteN) \n" )                          
                print(self.styleP("3:", style="bold"), " Différence (modalite1 - modalite2 - .... - modaliteN) \n") 
                print(self.styleP("4:", style="bold"), " Division (modalite1/modalite2) ou [ (modalite1 + modalite2 + .... + modaliteN-1)/modaliteN ] \n")    

                for i in range(nb_ind2):
                    print(f"""
    Indicateur {i+1}: """)   

                    print("""
    Quel type de calcul souhaitez-vous effectuer? \n""")
                    print(self.styleP("""
    Veuillez entrez un numéro parmis les choix qui vous sont proposés ci-dessus: \n""", style="bold"))                       
                                                                
                    type_calcul2 = None  ############ input() int!         
                    time.sleep(1)
                    # ajout condition while pour exceptions
                    while type_calcul2 not in ['1', '2', '3', '4']:
                        type_calcul2 = input()
                        if type_calcul2.strip() not in ['1', '2', '3', '4']:
                            print(self.styleP("""
    Veuillez saisir une des valeurs mentionnées  """, color='red'))
                            type_calcul2 = None

                    colonnes_t1 = self.list_col_table(nom)
                    colonnes_t2 = self.list_col_table(nom2)   # ('_'.join(nom2))
                    time.sleep(1)
                    while True:
                        saisie_colonnes = input("""
    Veuillez saisir les modalités pour le calcul (séparées par des virgules) : """)
                        if re.match(r'^\w+(,\s*\w+)*$', saisie_colonnes):
                            colonnes = [colonne.strip().lower() for colonne in saisie_colonnes.split(',')]

                            colonnes_fin = []
                            ok = True

                            for col in colonnes:
                                if col in colonnes_t1:
                                    colonnes_fin.append(f"t1.{col}")
                                elif col in colonnes_t2:
                                    colonnes_fin.append(f"t2.{col}")
                                else:
                                    print(self.styleP("""
    Attention: La colonne '{}' n'existe dans aucune des tables spécifiées.""",color='red').format(col))
                                    ok = False
                            if ok:
                                colonnes = ', '.join(colonnes_fin)
                                colonnes= [colonne.strip().lower() for colonne in colonnes.split(',')]
                                break
                    # Joindre les colonnes formatées en une seule chaîne
                            
                            # print(colonnes)
                            
                            # break
                        else:
                            print(self.styleP("""
    Les modalités doivent être séparées par une virgule ! """, color='red'))
                            
                    nom_ind2 = "" #input(""" Choisissez un nom pour l\'indicateur à créer :  \n""" )  

                    # ajout boucle while pour gérer les exceptions
                    while nom_ind2.strip() == "":
                        nom_ind2 = input(""" 
    Choisissez un nom pour l\'indicateur à créer :  \n""" )
                        if nom_ind2.strip() == "":
                            print("Le nom de l'indicateur ne peut pas être vide.")
                            
                    #### Création de l'indicateur
                    
                    ## dictionnaire des modalités avec leur type pour la requête
                    info_ind2 = {"type": type_calcul2, "variables": colonnes, "nom_indicateur": nom_ind2}
                    infos_ind2.append(info_ind2)

                # Requête de la vue avec jointure
                # print(choix_nom2,self.echelle)
                sqlQuery2 = """ CREATE VIEW """ +self.nom_schema+'.I_'+choix_nom2 + """  AS SELECT t1.row_id, t1.annee ,""" +'t1.'+self.echelle + ""","""  # ICI   """ + nom+'.row_id' + """, """ + nom+'.annee' +"""
        
                for info_ind2 in infos_ind2:

                    type_calcul2 = info_ind2["type"]

                    variables = info_ind2["variables"]
            
                    # nom_indicateur = info_ind2["nom_indicateur"]

                    ##### définition des opérateurs par type de calcul

                    colonnesPlus = "+".join(info_ind2["variables"])
                    colonnesMoins= "-".join(info_ind2["variables"])
                    colonnesPart = "/".join(f"NULLIF({col}, 0)" for col in variables) 

                    nom_ind2 = info_ind2["nom_indicateur"]

                    if type_calcul2 == '2':
                        sqlQuery2 +=   f"CASE WHEN NOT SUM({colonnesPlus}) IS NULL  OR SUM({colonnesPlus}) != 0 THEN SUM({colonnesPlus}) ELSE NULL END AS {nom_ind2}, "

                    elif type_calcul2 == '3':
                        sqlQuery2 +=   f"CASE WHEN NOT SUM({colonnesMoins}) IS NULL OR SUM({colonnesMoins}) != 0 THEN SUM({colonnesMoins}) ELSE NULL END AS {nom_ind2}, "
            
                    elif type_calcul2 == '1':
                        somme_num_col = "+".join(variables[:-1])
                        denum_col = variables[-1]

                        sqlQuery2 += f"CASE WHEN NOT SUM({somme_num_col}) IS NULL AND SUM({denum_col}) IS NOT NULL AND SUM({denum_col}) != 0 THEN (SUM({somme_num_col}) / SUM({denum_col})) * 100 ELSE NULL END AS {nom_ind2}, "    

                    elif type_calcul2== '4':
                        somme_num_col = "+".join(variables[:-1])
                        denum_col = variables[-1]

                        sqlQuery2 += f"CASE WHEN NOT SUM({somme_num_col}) IS NULL AND SUM({denum_col}) IS NOT NULL AND SUM({denum_col}) != 0 THEN SUM({somme_num_col}) / SUM({denum_col})  ELSE NULL END AS {nom_ind2}, "   

                    
                sqlQuery2= sqlQuery2[:-2]

                sqlQuery2 += ' FROM  ' + self.nom_schema+'.'+nom + ' as t1 INNER JOIN ' + self.nom_schema+'.'+nom2 + """ 
                as t2 ON t1.row_id = t2.row_id
                GROUP BY  t1.row_id , t1.annee, """ +'t1.'+self.echelle+ """ ORDER BY t1.row_id ASC ; """

                sqlQuery2 = sqlQuery2.format(view_name=nom) 

                # print(sqlQuery2)
                try:
                    dropingLine = 'DROP VIEW  IF EXISTS ' + self.nom_schema+'.I_'+choix_nom2 + ';'  #+'_avec_Indicateur'      '_'.join(nom2)
                    cur.execute(dropingLine)
                    cur.execute(sqlQuery2)
                    self.con.commit()

                    cur.execute(f"""
                    INSERT INTO {self.nom_schema}.indicateurs_descriptions (nom_indicateur, description)
                    VALUES (%s, %s)
                    ON CONFLICT (nom_indicateur) DO UPDATE SET description = EXCLUDED.description;  
                    """, (self.nom_schema+'.I_'+choix_nom2, description))
                    self.con.commit()

                    print('+-'*80, '\n')
                    print(self.styleP("La nouvelle vue indicateur" + self.nom_schema+'.I_'+choix_nom2  + " a été créée avec succès dans PostgreSQL \n", style= 'bold', color= 'cyan'))
                    print('+-'*80, '\n')

                except (Exception) as error :
                    print ("Erreur lors de la création de la vue inidcateur"+ self.nom_schema+'.I_'+choix_nom2  +" dans PostgreSQL: ", error)
                    print(sqlQuery2)

                break

            else:
                print(self.styleP("""
    Vous n'avez que deux possibilités de choix ! \n 
    Veuillez choisir l'option 1 ou 2 \n
    """, color= 'red'))



    ##################################################################################################################################
    ### demander choix binaire
    ##################################################################################################################################

    def demander_choix_binaire(self, message):
        """
        Demande à l'utilisateur de faire un choix binaire (O/N) jusqu'à ce qu'une réponse valide soit donnée.
       
        Args:
            message (str): Le message à afficher pour demander à l'utilisateur.
           
        Returns:
            bool: True si l'utilisateur choisit 'O', False si l'utilisateur choisit 'N'.
        """
        input_ok = False
        while not input_ok:
            choix = input(message + " (O/N): ").strip().upper()
            if choix in ['O', 'N']:
                input_ok = True
                if choix == 'O':
                    print("Vous avez choisi: Oui")
                    return True
                else:
                    print("Vous avez choisi: Non")
                    return False
            else:
                print("Répondez par : O ou N")    

########################################################################################################
        
def mignon(*args):
    if len(args)==1:
        print(('Vous pouvez jouer à un petit jeu.\n'
                'Pour cela, remplacez le nombre entier n dans self.mignon(n)\n'
                'par 3 nombres entiers compris entre 1 et 2, séparées par des virgules (par exemple : self.mignon(1,2,1)).\n'
                'Chaque nombre représentera le nombre de vaches, de chats ou de cochons affichés,\n'
                'avec une marge d\'erreur (il y aura des animaux qui vont s\'inviter à la fête).\n' 
                'Votre objectif est de déterminer quel emplacement est utilisé pour les chats, les cochons et les vaches.\n'
                'Une fois que vous pensez avoir trouvé, '))


    miaou =  """
                      __________            
                     /  MIAOU   \     
                     \__________/       
                         /
                 /\_/\  /
                ( o.o )     
                 > ^ <       
             """
    meuh =  """            
             __________            
            /   MEUH   \     
            \__________/       
                   \      ^__^   
                    \     (oo)\______
                          (__)\      )\/\/
                              ||----w |
                              ||     ||
                    """       
    groin = """
                 __________            
                /   GROIN  \     
                \__________/       
                    /
                   /
             ^..^
            ( oo )  )~
             ,,  ,, 
                    
                    """
    animaux = [miaou, meuh, groin]
    try:
        if isinstance(args[0], int) and len(args)==1:
            for i in range(args[0]):
                animal = random.randint(0, 2)
                print(animaux[animal])
        elif isinstance(args, tuple) and len(args)==3:
            if 0 in args:
                print("Haha je vous ai vu venir !\nVeuillez renseigner 3 entiers naturels séparés par des virgules entre 1 et 2.")
            if any(arg >= 3 for arg in args):
                print('Entre 1 et 2 on a dit !')
            else:
                animals = [groin for i in range(args[0])] + [meuh for i in range(args[1])] + [miaou for i in range(args[2])]
                animals = animals + [animaux[random.randint(0,2)]] #hihi
                random.shuffle(animals)
                for i in range(len(animals)):
                    print(animals[i])
    except:
        print("Veuillez renseigner 3 entiers naturels non nuls séparés par des virgules.")
