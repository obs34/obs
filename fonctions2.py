#!/usr/bin/env python
# coding: utf-8

# In[ ]:

# CHANGEMENT GLOBAL
# Nom_base ==> database_pg 
# nom_Fichier_Modalite ==> nom_csv_modalite
# nom_Fichier_Variable ==> nom_csv_variable

# CHANGEMENT FUTUR
# path_var en global
# nom_schema dans la fonction choix_observatoire() pour restreindre et clarifier

# col_names mod, val, var en variable global pour clarifier versementdonnees2


##############################################################################################################################
############# changement à effectuer dans fichier .xlsx
##############################################################################################################################

def modiFichier(nom_Excel):
     
    """
    Résultat : Affiche à l'utilisateur les modification qu'il a à apporter au fichier à traiter 
   
    """
    import pandas as pd

    nouvelles_donnees = {}
     
    #df = pd.ExcelFile(str(nom_Excel))
    df = pd.read_excel(nom_Excel, sheet_name=None, engine='openpyxl') 

    for sheet, dataframe in df.items():
    # supprimer les caractères spéciaux et les espaces dans les noms de colonnes
        dataframe.columns = dataframe.columns.str.replace(r'[\(\)\+\-\>\<\?\.]', '', regex=True).str.strip()
        dataframe.columns = dataframe.columns.str.replace(' ', '')
        print("Plus aucun caractère inattendu dans le nom des colonnes...")

        for col in dataframe.columns:
            dataframe[col] = dataframe[col].apply(lambda x: '' if isinstance(x, str) and x.isalpha() else x)

        nouvelles_donnees[sheet] = dataframe
        
        nouveau_nom_Excel = nom_Excel

        with pd.ExcelWriter(nouveau_nom_Excel, engine='openpyxl') as writer:
            for sheet_name, data in nouvelles_donnees.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
                
        print('_____________________________________________________________________________________')
        print(styleP("Nouveau fichier Excel enregistré avec les modifications à apporter pour le versement.", color='purple', style= 'bold') )
        print('_____________________________________________________________________________________')   



    # for sheet in df.sheet_names:
    #     print(sheet)
    #     dataframe = df.parse(sheet)

    #     dataf = pd.read_excel(df, sheet_name=sheet, engine='openpyxl')
    # #    colonnes = dataframe.columns
    # #   print(colonnes)

    #     if any(dataframe.columns.str.contains(r'[\(\)\+\-\>\<\?\.]', regex=True)):
    #         dataframe.columns = dataframe.columns.str.replace(r'[\(\)\+\-\>\<\?\.]', '', regex=True).str.strip() 
    #         dataframe.columns = dataframe.columns.str.replace(' ', '')
    #         print("Plus aucun caractère inattendu dans le nom des colonnes...")
    #         print(dataframe.columns)

    #     nouvelles_donnees[sheet] = dataframe

    #     for index, row in dataf.iloc[1:].iterrows():
    #     # Remplacer les valeurs alphanumériques par rien
    #         dataf.iloc[index] = row.apply(lambda x: '' if isinstance(x, str) and x.isalpha() else x)   

    # # if not df_original.equals(df):
    # #     print("Les valeurs secrétisées du jeu de données ont été corrigées.")
    # # else:
    # #     print("Le jeu de données ne contient pas de données secrétiseés.")


    # # Créer un nouveau fichier Excel avec les nouvelles données
    #     nouveau_nom_Excel = nom_Excel
    #     dataf.to_excel(nouveau_nom_Excel, index=False)
    #     dataf.info()

    # # writer = pd.ExcelWriter(nouveau_nom_Excel)

    # # # # écrire les données modifiées 
    # # for sheet, dataf in nouvelles_donnees.items():
    # #     dataf.to_excel(writer, sheet_name=sheet, index=False)

    # # # sauvegarde
    # #     writer.save()

    # df.close()

    # print('_____________________________________________________________________________________')
    # print(styleP("Nouveau fichier Excel enregistré avec les modifications à apporter pour le versement.", color='purple', style= 'bold') )
    # print('_____________________________________________________________________________________')   




# Réduire le nombre de connexion
##############################################################################################################################
############# Connexion OK
##############################################################################################################################

def connexion(): #database_pg, Nom_utilisateur, mot_de_passe, nom_host, port
     
    """
    Données : database_pg est un string qui represente le nom de la base de données à la quelle on souhaite se connecter
              Nom_utilisateur est un string qui represente le nom de pgAdmin de l'utilisateur qui souhaite de connecter
              mot_de_passe est un string qui represente le mot de passe de l'utilisateur
              nom_host est un string qui represente le nom de l'hebergeur de la base de données
              port est un string numerique qui represente le port de connexion à la base de données
              
    Résultat : Affiche une phrase (string) indiquant si la connexion est réussie ou non et retourne con (à re-définir)
    """
    import psycopg2
    
    try:
        # connexion
        con = psycopg2.connect(database=database_pg, user=Nom_utilisateur, password=mot_de_passe, host=nom_host, port=port)
        #print("Connexion réussie \n")
        return con
    

    except (Exception, psycopg2.Error) as error :
        print ("Erreur lors de la connexion à PostgreSQL", error)


##################################################################################################################################
### Choix de l'observatoire
##################################################################################################################################

def choix_observatoire(): # new

    import psycopg2
    import getpass

    """
    Données : database_pg est un string qui represente le nom de la base de données à la quelle on souhaite se connecter
              Nom_utilisateur est un string qui represente le nom de pgAdmin de l'utilisateur qui souhaite de connecter
              mot_de_passe est un string qui represente le mot de passe de l'utilisateur
              nom_host est un string qui represente le nom de l'hebergeur de la base de données
              port est un string numerique qui represente le port de connexion à la base de données
              nom_schema représente le nom du schéma dans lequel le jeu de donées sera versé 
              
    Résultat : 

    Remarque : 
    """
    observatoire_ok=False

    # définition des variables globales
    global database_pg
    global Nom_utilisateur
    global mot_de_passe
    global nom_host
    global port
    global nom_schema

    global nom_obs
    while(not observatoire_ok):
        print(""" 
Demande connexion! \n
Pour vous connecter veuillez renseigner le mot de passe associé à votre observatoire
                    """)
        print(""" 
Veuillez choisir l'observatoire auquel vous êtes rattaché :\n
            1 pour ODH
            2 pour MDDEP
                        \n""")
        observatoire = input()
        if(int(observatoire) == 1):
            nom_obs = 'l\'ODH'
            observatoire_ok = True
            database_pg = "bdsociohab"
            Nom_utilisateur = "admoddh"
            mot_de_passe =  getpass.getpass("Entrez votre mot de passe : ") #input("Entrez le mot de passe: ")                                
            nom_host = "s934"
            port = "5441"
            nom_schema = "socio_hab_test".lower()
        elif(int(observatoire) == 2):
            nom_obs = 'la MDDP'
            observatoire_ok = True
            database_pg = "bdsociohab"
            Nom_utilisateur = "admmddep"
            mot_de_passe = getpass.getpass("Entrez votre mot de passe : ") #input("Entrez le mot de passe: ")                                  
            nom_host = "s934"
            port = "5441"
            nom_schema = "test"
        else:
            print('Veuillez répondre par un entier étant présent ci-dessus')
            observatoire_ok = False

        try:
        # connexion
            con = psycopg2.connect(database=database_pg, user=Nom_utilisateur, password=mot_de_passe, host=nom_host, port=port)
            # print(colorize_text("Texte en italique et bleu", style="italic", color="blue"))
            print(styleP("Connexion réussie \n", color= "green"))
            print(styleP(f'Vous avez choisis {nom_obs}, vous vous apprêter actuellement à faire votre versement dans\nla database {database_pg} et le schéma {nom_schema}', style = 'bold'))
            return con


        except (Exception, psycopg2.Error) as error :
            print(styleP("Erreur lors de la connexion à PostgreSQL", color= "red") )   
            
    return nom_schema


##############################################################################################################################
############# Checking variables
##############################################################################################################################

def checkingVariable(csv_files,source):
    '''
    Données : csv_files = liste des noms des feuilles du fichier Excel que l'on cherche à verser dans pg
                          Ces noms étant listés dans csv_files grâce à la fonction liste_feuille()    
              source = la source de provenance des données ex: INSEE, CEREMA,...             
              nom_csv_variable est le nom exacte du fichier CSV des variables de cette source  et le nom de la table dans pg (sans .csv)           

    Résultat : retourne la liste des variables avec toutes les eventuelles nouvelles variables et créé le fichier 
                var.csv associé qui sera versé sur pg
    '''
    import pandas as pd
    import unidecode

    global nom_csv_variable
    global path_vars

    # Lecture du fichier Variables_.csv', qui se trouve dans le dossier 'vars'
    Variables = pd.read_csv(path_vars+nom_csv_variable, delimiter=';', encoding='utf-8-sig') #'utf-8-sig' pour forcer
    Variables_nouv = pd.DataFrame(columns=['CodeVarEXT','LibAxeAnalyse','Origine','CodeVarID'])

    # checking 
    for Variable in range(len(csv_files)):
        print('variable recherchée : {}'.format(csv_files[Variable], '\n'))
        
        if(unidecode.unidecode(csv_files[Variable]).lower() in Variables['CodeVarEXT'].tolist() ):
              
            print(styleP('trouvée \n', color='green'))                            
        else:
            print(styleP('nouvelle \n', color= 'green'))

            if choix_input == 'OUI':
                                   
                print('Veuillez saisir le libellé long de cette variable')
                lib = input()
                if(lib == ''):
                    lib = csv_files[Variable]

            else:
                lib = ''    
            # insertion dans le dataframe des variables
            #changement
            Variables_nouv = pd.concat([
                    Variables_nouv, pd.DataFrame.from_records([{ 
                        'CodeVarID' : len(Variables)+1,  #len(Variables) nombre de variable dans la table var et +1 parce qu'on en rajoute
                        'CodeVarEXT' : unidecode.unidecode(csv_files[Variable]).lower(),
                        'LibAxeAnalyse' : lib,
                        'Origine' : source.lower() 
                    }])
                ])

            Variables = pd.concat([ 
                    Variables, pd.DataFrame.from_records([{
                        'CodeVarID' : len(Variables)+1,
                        'CodeVarEXT' : unidecode.unidecode(csv_files[Variable]).lower(),
                        'LibAxeAnalyse' : lib,
                        'Origine' : source.lower() 
                    }])
                ])
            print('la variable {} à été insérée \n '.format(csv_files[Variable]))
           
        # enregistrement des modifs
        Variables_nouv.to_csv(path_vars+'new_'+nom_csv_variable,index=False,sep=';', encoding='utf-8-sig')
        Variables.to_csv(path_vars+nom_csv_variable,index=False,sep=';', encoding='utf-8-sig')

##############################################################################################################################
############# Remplacer caractères spéciaux
##############################################################################################################################

def remplacerCaratereSpeciaux(fichier_A_traiter):
    """
    Donnée : fichier_A_traiter est le fichier (csv ou les sheets excel) original à traiter          
    Résultat : retourne fichier_A_traiter avec des noms de colonnes sans caractères spéciaux
    """
    
    # création de la liste des noms de colonnes de fichier_A_traiter
    listeCols = fichier_A_traiter.columns
    listeCols.tolist()
    
    tabNewColonnes = []
    
    for i in range(len(listeCols)):
        new_name = listeCols[i]
        string1 = new_name[0]
        if(string1.isnumeric()):
            new_name = new_name.replace(string1, '_'+string1,1)   
        if(('+') in new_name):
            new_name = new_name.replace('+', 'plus')
        if(('-') in new_name):
            new_name = new_name.replace('-', 'moins')
        if(('<') in new_name):
            new_name = new_name.replace('<', 'inf')           
        if(('>') in new_name):
            new_name = new_name.replace('>', 'sup')           
        if(('=') in new_name):
            new_name = new_name.replace('=', 'egal')  

        # adding for () 
        if((')') in new_name):
            new_name = new_name.replace(')', '')
        if(('(') in new_name):
            new_name = new_name.replace('(', '')    


        tabNewColonnes.append(new_name)

    
    fichier_A_traiter.columns = tabNewColonnes

##############################################################################################################################
############# Remplacer space modalités
##############################################################################################################################

def replacingSpaceModa(listeCols): # listeCols est la liste des modalités
     
    #transformer les charactères en unicode

    import unidecode

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
############# Checking modalités
##############################################################################################################################

def checkingModalites(fichier_A_traiter):
    
    """
    Donnée : fichier_A_traiter est le fichier (csv ou les sheets excel) original à traiter 
             nom_csv_modalite est le nom exacte du fichier CSV des modalités de cette source et le nom de la table dans pg (sans .csv)         
             path_vars est le chemin d'accès au dossier "vars"
                       
    Résultat : retourne la liste des modalités avec toutes les eventuelles nouvelles modalités et créé le fichier 
               mod.csv associé qui sera versé sur pg
    """
    import pandas as pd

    global Modalites_nouv
    global nom_csv_modalite
    global path_vars
    Modalites = pd.read_csv(path_vars+nom_csv_modalite, delimiter=';', encoding='utf-8-sig')
    
    #Liste des modalités et de leurs codes
    listeCodeModa = pd.DataFrame(columns = ['code', 'noms', 'Lib_long','categ_regroupement'])    
    #remplacement des caractères spéciaux
    remplacerCaratereSpeciaux(fichier_A_traiter)
    
    #transformer les charactères en unicode et les premiers lettres en capitales (avec la fonction .title())
    fichier_A_traiter.columns = replacingSpaceModa(fichier_A_traiter.columns)

    # checking   
    for modalite in range(len(fichier_A_traiter.columns)):  ## if PB
        print('Modalité recherchée :', fichier_A_traiter.columns[modalite], '\n')

        # test
        if(fichier_A_traiter.columns[modalite].lower() in Modalites['CodeModalEXT'].tolist() ):
            
            print(styleP('trouvé \n', color= 'green'))
            ligne = Modalites.loc[(Modalites['CodeModalEXT'] == fichier_A_traiter.columns[modalite].lower())]
            ligne = ligne.reset_index(drop=True)

            listeCodeModa = pd.concat([
                listeCodeModa, pd.DataFrame.from_records([{
                    'code': ligne.CodeModalID[0],
                    'noms': fichier_A_traiter.columns[modalite],
                    'Lib_long': ligne.Libelle_long[0],
                    'categ_regroupement': ligne.Categ_regroupement[0] 
                }])
            ])
        else:
            print(styleP('nouveau \n', color='green'))

            if choix_input == 'OUI':

                print('Veuillez saisir le libellé long de cette modalité')
                # faire en sorte qu'on puisse revenir en arrière
                lib = input()
                print('Veuillez saisir la catégorie de regroupement de cette modalité')
                categ_regroupement = input()
                ### possibilité d'ajout de la fonction pour contrôler la saisie
                categ_regroupement = replacingSpaceInput(categ_regroupement)     ### ajout de la fonction

                if(categ_regroupement == ''):
                    categ_regroupement = fichier_A_traiter.columns[modalite]

                ### possibilité d'ajout de la fonction pour contrôler la saisie
            else:
                lib = ''
                categ_regroupement = ''

            listeCodeModa = pd.concat([
                listeCodeModa, pd.DataFrame.from_records([{
                    'code': len(Modalites)+1, #len(modalites) nombre de modalites dans la table modalites parce qu'on en rajoute
                    'noms': fichier_A_traiter.columns[modalite],
                    'Lib_long': lib,
                    'categ_regroupement': categ_regroupement 
                }])
            ])
            #changement
            Modalites_nouv = pd.concat([
                Modalites_nouv, pd.DataFrame.from_records([{
                    'CodeModalID' : len(Modalites)+1, #len(modalites) nombre de d'anciennes modalites dans la table modalites, len(Modalites_nouv) : nombre de nouvelle modalite et +1 parce qu'on en rajoute
                    'CodeModalEXT' : fichier_A_traiter.columns[modalite].lower(),
                    'LibelleModal' : fichier_A_traiter.columns[modalite],
                    'Libelle_long' : lib,
                    'Categ_regroupement': categ_regroupement 
                }])
            ])


            Modalites = pd.concat([
                Modalites, pd.DataFrame.from_records([{
                    'CodeModalID' : len(Modalites)+1, #len(modalites) nombre de d'anciennes modalites dans la table modalites, len(Modalites_nouv) : nombre de nouvelle modalite et +1 parce qu'on en rajoute
                    'CodeModalEXT' : fichier_A_traiter.columns[modalite].lower(),
                    'LibelleModal' : fichier_A_traiter.columns[modalite],
                    'Libelle_long' : lib,
                    'Categ_regroupement': categ_regroupement 
                }])
            ])

    Modalites.fillna('NULL', inplace=True)
    Modalites_nouv.fillna('NULL', inplace=True)

    Modalites.to_csv(path_vars+nom_csv_modalite,index=False,sep=';', encoding='utf-8-sig') 
    Modalites_nouv.to_csv(path_vars+'new_'+nom_csv_modalite,index=False,sep=';', encoding='utf-8-sig') 

    return listeCodeModa


##############################################################################################################################
############# Checking input
##############################################################################################################################

def replacingSpaceInput(entree):

    """
    Donnée   : 

    Résultat :

    """

    import unidecode

    # enlever les accents
    correct_entree = unidecode.unidecode(entree).title()

    # sup les caractères spéciaux
    correct_entree = correct_entree.replace(',', '').replace('.', '').replace('(', '').replace(')', '').replace('/', '_').replace("'", '_').replace('+', '_').replace('-','_').replace('*','_').replace('/', '_')

    correct_entree = correct_entree.replace(' ', '_')

    return correct_entree.lower()

##############################################################################################################################
############# Traitement données
##############################################################################################################################

def traitementDonnees(fichier_A_traiter,codevarid,annee):
    """
    Donnée : fichier_A_traiter est le fichier (csv ou une feuille Excel) original à traiter  
             nomFichier est un string représentant le nom du fichier "fichier_A_traité"           
             codevarid est un int qui represente l'identifiant de la variable traité dans le fichier "fichier_A_traité"            
             annee est l'année de récolte du millésime des données   
             
    Résultat : un nouveau dataframe panda newTabTraite contenant la table des valeurs prêt à être exporter vers la base de données sur pgAdmin
    Remarque :  
    """
    
    #Importations des packages nécessaires pour traiter les fichiers
    import pandas as pd
    import datetime


    current_datetime = datetime.datetime.now()

    current_date = current_datetime.isoformat()  #datetime.date.today().isoformat()

    #Création de la table à exporter plus tard
    newTabTraite = pd.DataFrame(columns = ['CodeVarID', 'Annee', 'codeEntite', 
                                 'CodeModalID', 'Echelle',
                                 'IndicePosition', 'Valeur', 'Date_versement']) 
  
    tabATraite = fichier_A_traiter

    # pour avoir la liste des modalitées
    listeCodeModa = checkingModalites(tabATraite)
    
    #renommer la premier colonne en "codeEntite"
    tabATraite.rename(columns={ tabATraite.columns[0]: 'codeEntite'}, inplace = True)

    for ligne in range(len(tabATraite)):
        valeur = []
        codeModa = []
        code_ent = tabATraite['codeEntite'][ligne]       

        for col in range(1,len(listeCodeModa),1):
            valeur.append(tabATraite.iloc[ligne][col])
            codeModa.append(listeCodeModa.iloc[col]['code'])

        for i in range(len(valeur)):

            newTabTraite = pd.concat([
                newTabTraite, pd.DataFrame.from_records([{ 
                    'CodeVarID':codevarid,
                    'Annee':annee,
                    'Echelle': echelle,
                    
                    'codeEntite':code_ent,
                    'CodeModalID':codeModa[i],
                    'Valeur': valeur[i],
                    'IndicePosition': str(code_ent)+'0'+str(codeModa[i])+str(annee)+'0'+str(codevarid), #VERIFIER
                    'Date_versement' : current_date
                }])
            ])

        #Remplacement des cellules vides par des zeros
        #newTabTraite.fillna('NULL', inplace=True)       
    
    return newTabTraite

##############################################################################################################################
############# Traitement données complet
##############################################################################################################################

def traitementDonneesComplet(nom_Excel,annee,source,base,theme): 
    """
        Donnée : 
                annee est l'année de récolte du millésime des données               
                chemin = le nom du dossier où se trouve le fichier csv  traité  dans dossierfichierCSVtraite          
                nom_csv_modalite est le nom exacte du fichier CSV des modalités 
                source = la source de provenance des données ex: INSEE, CEREMA,...  
                nom_csv_variable est le nom exacte du fichier CSV des variables

        Résultat : 
                  Etape 1: Création du dossier vars
                  Etape 2: Importation, si elles existent, des tables mod et var du schéma choisi selon son observatoire.
                           Sinon créé les .csv vides correspondants
                  Etape 3: Choix de l'échelle manuellement
                  Etape 4: Extraction des noms de variables et utilistaion de la fonction checkingVariable() pour créér le fichier var à exporter vers pg
                  Etape 5: Création de DossierFichiersTraités et du fichier de valeurs à exporter vers pg
    """
    ####### IMPORT #######
    import glob
    import pandas as pd
    import os
    import shutil # to move files 
    import pathlib # to get the path of files
    import fonctions2
    from tqdm import tqdm

    ##### ETAPE 1: Création des dossiers temporaires et des variables globales #####

    os.mkdir('vars')
    os.mkdir('DossierFichiersTraités')
    os.mkdir('DossierFichiersbrutesCsv')

    global path_vars
    path_vars = 'vars/'

    global nom_csv_variable
    nom_csv_variable = 'aa_{}_var.csv'.format(nom_schema).lower()

    global nom_csv_modalite
    nom_csv_modalite = 'aa_{}_mod.csv'.format(nom_schema).lower()

    global val_col_names
    val_col_names = ['CodeVarID', 'Annee', 'codeEntite', 'CodeModalID', 'Echelle', 'IndicePosition', 'Valeur', 'Date_versement']

    global var_col_names
    var_col_names = ['CodeVarEXT','LibAxeAnalyse','Origine','CodeVarID']

    global mod_col_names
    mod_col_names = ['LibelleModal','CodeModalID','CodeModalEXT','Categ_regroupement','Libelle_long']

    global Modalites_nouv
    Modalites_nouv = pd.DataFrame(columns=['LibelleModal','CodeModalID','CodeModalEXT','Categ_regroupement','Libelle_long'])

    df = pd.ExcelFile(str(nom_Excel))    
    for sheet in df.sheet_names:
        df2 = pd.read_excel(df,sheet_name=sheet) 
        name = sheet 
        df2.to_csv('DossierFichiersbrutesCsv/'+name+".csv",index=False,sep=';', encoding='utf-8-sig')

    #choix de l'echelle des données ==> doit etre en while / Rajouter automatique
    global echelle
    echelle_ok=False
    while(not echelle_ok):
        print(""" Veuillez Choisir l'échelle correspondant à l'échelle de vos données :\n
                        1 pour Commune
                        2 pour EPCI
                        3 pour IRIS
                        4 pour Canton
                        5 pour Logement
                        6 pour Parcelle
                        7 pour Section cadastrale
                        8 pour Département
                        9 pour Autre

                        """)
        echelleIndicatif = input()
        if(int(echelleIndicatif) == 1):
            echelle = 'Commune'
            echelle_ok = True
        elif(int(echelleIndicatif) == 2):
            echelle = 'EPCI'
            echelle_ok = True
        elif(int(echelleIndicatif) == 3):
            echelle = 'IRIS'
            echelle_ok = True
        elif(int(echelleIndicatif) == 4):
            echelle = 'Canton'
            echelle_ok = True
        elif(int(echelleIndicatif) == 5):
            echelle = 'Logement'
            echelle_ok = True
        elif(int(echelleIndicatif) == 6):
            echelle = 'Parcelle'
            echelle_ok = True
        elif(int(echelleIndicatif) == 7):
            echelle = 'Section cadastrale'
            echelle_ok = True
        elif(int(echelleIndicatif) == 8):
            echelle = 'Département'
            echelle_ok = True
        elif(int(echelleIndicatif) == 9):
            print(" Veuillez saisir l'echelle correspondant à l'echelle de vos données :\n")
            echelleSaisie = input()
            echelle = echelleSaisie
            echelle_ok = True
        else:
            print('Réponse Invalide')
            echelle_ok = False  

        ########################## adding ici modif Enzo

    global choix_input
    input_ok=False
    while(not input_ok):
        #print(""" Est-ce les données que vous voulez input ? \n(Répondez par : OUI ou NON)""")
        print(""" Souhaitez-vous saisir les libellés et catégorie de regroupement pour toutes vos nouvelles modalités ? \n(Répondez par : OUI ou NON)""")
        choix_input = input()
        if(choix_input == 'NON'):
            input_ok = True
            choix_input = 'NON'
        elif(choix_input == 'OUI'):
            input_ok = True
            choix_input = 'OUI'
        else:
            input_ok = False

        ################ end modifs Enzo    

    ########## Peut-être créer une fonction pour ça = clarification
    # Vérifier l'existence de la table
    con = fonctions2.connexion()
    if fonctions2.table_exists(con, f'aa_{nom_schema}_var') and fonctions2.table_exists(con, f'aa_{nom_schema}_mod'): #changement
        print(styleP('Test existence de var et mod réussie', color= 'blue'))
        
        fonctions2.importe('var')
        fonctions2.importe('mod')
                                                     
    else:
        print(styleP('Création d\'un observatoire !', color= 'blue')) #ICI#
                                                                    
        mod = pd.DataFrame(columns=['LibelleModal','CodeModalID','CodeModalEXT','Categ_regroupement','Libelle_long'])
        mod.to_csv(path_vars+nom_csv_modalite,index=False,sep=';', encoding='utf-8-sig')

        var = pd.DataFrame(columns=['CodeVarEXT','LibAxeAnalyse','Origine','CodeVarID'])            
        var.to_csv(path_vars+nom_csv_variable,index=False,sep=';', encoding='utf-8-sig')
                                                                   


    ########################### get all csv files ########################################
    chemin = 'DossierFichiersbrutesCsv' #chemin = le nom du dossier où se trouve le fichier csv à traiter

    cheminCsv = chemin+'/*.csv'

    csv_files = glob.glob(cheminCsv)
    feuilles = [pd.read_csv(x, sep=';', encoding='utf-8-sig') for x in csv_files]

    #removing "DossierFichiersbrutesCsv\\" files's names 
    for variable in range(len(csv_files)):

        csv_files[variable]= csv_files[variable].replace(chemin,'')
        csv_files[variable]= csv_files[variable].replace('//','')
        csv_files[variable]= csv_files[variable].replace('/','')
        csv_files[variable]= csv_files[variable].replace('\\','')

        csv_files[variable]= csv_files[variable].replace('.csv','')

    ############## CHECKING LA LISTE DES VAR ##########################   
    fonctions2.checkingVariable(csv_files,source)
    var = pd.read_csv(path_vars+nom_csv_variable, delimiter=';', encoding='utf-8-sig')
    
    ############## CREATION DE LA TABLE A EXPORTER PLUS TARD ################
    csv_files_sans_accent_ni_escpace = fonctions2.replacingSpaceModa(csv_files)
    
    finalTabTraite = pd.DataFrame(columns = ['CodeVarID', 'Annee', 'codeEntite', 
                                 'CodeModalID', 'Echelle', 
                                 'IndicePosition', 'Valeur', 'Date_versement']) #changement
    
    for feuille in tqdm(range(len(feuilles))):
        #print(file, "file")
        nomVariable = csv_files_sans_accent_ni_escpace[feuille]
        print(' nom ::::: ', nomVariable)
        fichier_A_traiter = feuilles[feuille]
        var_loc = var.loc[var['CodeVarEXT'].str.contains(nomVariable, case=False)]
        var_loc.reset_index(drop=True, inplace=True)
        codevarid = var_loc['CodeVarID'][0] 
           
        newTabTraite = fonctions2.traitementDonnees(fichier_A_traiter,codevarid,annee)
        finalTabTraite = pd.concat([finalTabTraite, newTabTraite])
        
    newName = theme + '_' + base + '_' + source + '.csv'   
    finalTabTraite.fillna('NULL', inplace=True)    
    pd.DataFrame(finalTabTraite).to_csv(newName, index=False,encoding='utf-8-sig',sep=';')  

    #localisation
    currentPath = pathlib.Path().absolute()
    destination = currentPath/'DossierFichiersTraités'
    #localistion du fichier traité
    localisation = currentPath/newName

    # deplacer le fichier traité vers le dossier des fichiers traités
    shutil.move(str(localisation), str(destination))
    
##############################################################################################################################
############# Création table avec fichier complet
##############################################################################################################################

def creationTableAvecFichierComplet(fichierCSV, nom, nom_schema, con, nbclE, CleFK, ClePK, tableRef): #ICI# (- path_vars)
    """
    données : fichierCsv est un fichier CSV dont on veut reproduire la structure sur une base de données  
              nom est un string qui représente le nom du fichier             
              nom_schema représente le nom du schéma dans lequel le jeu de donées sera versé             
              con = une instance de la fonction connexion         
              nbclE est le nombre (en int) de clés étrangères              
              CleFK est une liste des clés étrangères             
              ClePK est un string qui représente la clée primaire              
              tableRef est une liste des tables de références des clés étrangères            
              (Dossier est un string qui indique le nom du dossier dans lequel se trouve le fichier)              
              path_vars est le chemin du dossier "vars"
              
    résultats : créé une table vide suivant la structure de fichierCsv en définissant la clé primaire pour chaque table ainsi que 
                le clés étrangères si elles sont définies dans infoMeta. 
                Affiche une phrase indiquant si la table à été créée ou pas et retourne le nom de la table créée
                
    remarques : il faut déja se connecter avec la fonction 'connexion'
    """
     # importation de packages
    import psycopg2
    import numpy as np
    
    global path_vars #ICI#

    newTab = fichierCSV
    con = con
    
    # dictionnaire des metas infos à remplir
    infoMeta = {
        'nom':[],
        'colonnes': [],
        'primary_key' : [],
        'foreign_Keys' : [],
        'tableRefFK' : []
    }
    # nom de la table à créer
    nom=nom.replace(path_vars,'')
    nom=nom.replace('.csv','') 
    infoMeta['nom'].append(nom.lower())
    
    #noms colonnes de la nouvelle table
    newTabCol =  newTab.columns
    for col in range(len(newTabCol)):
        nomCol = newTabCol[col]
        infoMeta['colonnes'].append(nomCol)
    InClePrim = ClePK

    # adding the primary key into infoMeta
    infoMeta['primary_key'].append(InClePrim)
    if(InClePrim == ''):
           print('pas de clé primaire')
           # max value de la clée primaire
           max_value = 0
    else:
        #indice de position de la clée primaire dans liste des colonnes
        j=0
        trouve = False
        indicePK= 'rien'
        while(j<=len(newTabCol) and trouve==False):
            #print(infoMeta['primary_key'])
            if (infoMeta['primary_key'][0]==newTabCol[j]):
                indicePK= j
                trouve = True
            j+=1
    nbcleEtran = int(nbclE)
    if (nbcleEtran != 0):
        # saisie des clées étrangères de la table
        InCleEtrang = CleFK
        for i in range(len(InCleEtrang)):
            InCleEtrang[i] = InCleEtrang[i].strip()
        # adding the foreign keys into infoMeta
        for i in range(len(InCleEtrang)):
            infoMeta['foreign_Keys'].append(InCleEtrang[i]) 
        # saisie des tables de références des clées étrangères
        InRefFK = tableRef
        for i in range(len(InRefFK)):
            InRefFK[i] = InRefFK[i].strip()
        # adding tab refs 
        for i in range(len(InRefFK)):
            infoMeta['tableRefFK'].append(InRefFK[i])
        #indice de position des clées étrngères dans liste des colonnes
        for cleEtrangere in range(nbcleEtran):
            j=0
            trouve = False
            indicePK = 'rien'
            while(j<len(newTabCol) and trouve == False):
                if (infoMeta['foreign_Keys'][cleEtrangere] == newTabCol[j]):
                    #print('here')
                    indicePK = j 
                    trouve = True
                j+=1
    else :
        print('Pas de clé étrangère')
    #print(infoMeta)            
    # sql query 
    sqlQuery = str("CREATE TABLE "+ nom_schema+"."+infoMeta['nom'][0]+"(")
    lignesCommande=[]
    for l in range(len(infoMeta['colonnes'])):
        nom_colonne = infoMeta['colonnes'][l]
        type_colonne = fichierCSV[nom_colonne].dtype
        #print(type_colonne)
        ligne = str(infoMeta['colonnes'][l]+" ")  + map_pandas_to_postgres_type(type_colonne)
        #primary key query
        if(InClePrim != ''):
            if(infoMeta['primary_key'][0] == infoMeta['colonnes'][l]):
                ligne = ligne+" "+ 'PRIMARY KEY'               
        lignesCommande.append(ligne)  
    sqlQuery = sqlQuery +str(lignesCommande)
  
    # creation the query for a possible foreign keys
    table_sqlQF = []
    if (nbcleEtran != 0):
        for f in range(len(infoMeta['foreign_Keys'])):
            sqlQF = str('CONSTRAINT fk_')+str(infoMeta['tableRefFK'][f]+ ' FOREIGN KEY('+ infoMeta['foreign_Keys'][f]+ ') REFERENCES '+ nom_schema+"."+infoMeta['tableRefFK'][f]
                    +' ('+infoMeta['foreign_Keys'][f]+')')
            table_sqlQF.append(sqlQF)
        sqlQuery =sqlQuery + ','+ str(table_sqlQF)+')'        
    else:
        sqlQuery= sqlQuery + ')'
    sqlQuery = sqlQuery.replace('[','')
    sqlQuery = sqlQuery.replace(']','')
    sqlQuery = sqlQuery.replace("'","")       
    ## Création de la table  
    try:
        cur = con.cursor()
        # Creating Table infoMeta['nom'][0]
        cur.execute(sqlQuery)      
        con.commit()
        print('______________________________________________________________________________')
        print("La table "+infoMeta['nom'][0]+ " à été créée avec succès dans PostgreSQL \n")
    except (Exception, psycopg2.Error) as error :
        print ("Erreur lors de la création de la table "+infoMeta['nom'][0]+" dans PostgreSQL: ", error)
        print(sqlQuery)
    return infoMeta['nom']            


##############################################################################################################################
############# map pandas to sql type
##############################################################################################################################

def map_pandas_to_postgres_type(pandas_type): #changement

    # permet de convertir les types pandas en postgresql

    import pandas as pd

    if pandas_type == 'int32':
        return 'INTEGER'
    elif pandas_type == 'int64':
        return 'INTEGER'
    elif pandas_type == 'float32':
        return 'DOUBLE PRECISION'
    elif pandas_type == 'float64':
        return 'DOUBLE PRECISION'
    elif pandas_type == 'object':
        return 'TEXT'
    elif pandas_type == 'datetime64[ns]':
        return 'DATE'

    return str(pandas_type)


##############################################################################################################################
############# insertion données
##############################################################################################################################

def insertionDonnees(fichierCSVpath, con, table, sep): #changement
    """
    Données : fichierCsv est un fichier CSV dont on veut reproduire la structure sur une base de données
              con = une instance de la fonction connexion
              table = nom (en string) de la table où vont être insérées les données
              sep = separateur de text, could be : ';' or '\t' for exemple
            
    résultat : insere les données de fichierCSV dans la table "table", dans une base de données. 
               Affiche une phrase indiquant si les données ont été insérées ou pas
    remarques : cette fonction est dédiée au versement des tables modalités, variables (et "posséder" si elle est créée)
    """
    
    import psycopg2
    import traceback
    import csv

    try:
        
        cur = con.cursor()
        with open(fichierCSVpath, 'r', encoding="utf-8-sig") as file:
            reader = csv.reader(file, delimiter=sep)
            next(reader)# Skip the header row.
            
            # Insertion des données dans la table 
            for row in reader:
                values = [x if x!='NULL' else None for x in row]
                
                ## conditions selon les fichiers
                if(len(values)==4):           # à adapter selon nbre de colonnes du fichier variables
                    cur.execute( "INSERT INTO {} VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING".format(table),values)
                elif (len(values)==5):        # à adapter selon nbre de colonnes du fichier modalites
                    cur.execute( "INSERT INTO {} VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING".format(table),values)
                elif (len(values)==8):        # à adapter selon nbre de colonnes du fichier valeur
                    cur.execute( "INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING".format(table),values)

        print(styleP('la requête a été executée', color='green'))
        
    except (Exception, NameError, psycopg2.Error) as error :
        print(traceback.format_exc())
        print ("Erreur lors de l'insertion des données importées dans la table {} :".format(str(table)), error)
        con.rollback()        
        
    else:
        con.commit()
        print("Les données importées ont été inserées avec succès dans la table {} \n".format(str(table)))


#############################################################################################################################
############# Versement donnees 2
##############################################################################################################################

def versementDonnes2(source,base,theme): #changement
    
    """
    Entrée : la source, la base et le thème pour le nom de la table valeur.
    Sortie : Pour les tables valeur, variable et modalité, créer la table si
    elle n'existe pas avec la fonction creationTableAvecFichierComplet
    et verse les données avec la fonction insertionDonnees
    """

    ######## IMPORT ########
    import glob
    import pandas as pd
    import fonctions2
    import psycopg2
    import traceback
    from tqdm import tqdm

    global nom_schema
    global nom_csv_modalite
    global nom_csv_variable
    global path_vars

    csv_files, path = glob.glob('DossierFichiersTraités/*.csv'), glob.glob('DossierFichiersTraités/*.csv')

    variable = nom_csv_variable.replace('.csv','')
    modalite = nom_csv_modalite.replace('.csv','')
    
    # versementDonnees(mod)
    # versementDonnees(var)
    # versementDonnees(val)
    ########## VARIABLES ################
    con = fonctions2.connexion()
    if not fonctions2.table_exists(con, f'aa_{nom_schema}_var'):
        nom = glob.glob(path_vars+nom_csv_variable)[0]   
        print(styleP('Création de la table variable \n', color= 'purple'))
        nbclE = 0
        CleFK = []
        ClePK = 'CodeVarID'
        tableRef = []

        # attribution du fichier Variables.csv à la variable fichierCSV
        fichierCSV = pd.read_csv(nom, delimiter=';', encoding='utf-8-sig')

        fichierCSV['CodeVarEXT'] = fichierCSV['CodeVarEXT'].astype(str)
        fichierCSV['CodeVarID'] = fichierCSV['CodeVarID'].astype(int)   
        fichierCSV['LibAxeAnalyse'] = fichierCSV['LibAxeAnalyse'].astype(str)
        fichierCSV['Origine'] = fichierCSV['Origine'].astype(str)

        con = fonctions2.connexion()
        fonctions2.creationTableAvecFichierComplet(fichierCSV, nom, nom_schema, con, nbclE, CleFK, ClePK, tableRef)

    print(styleP('Insertion des données dans la table variable \n', style= 'bold'))
    nom = glob.glob(path_vars+'new_'+nom_csv_variable)[0]
    try:
        con = fonctions2.connexion()
        table = nom_schema+"."+variable.lower()
        sep = ';'
        fonctions2.insertionDonnees(nom, con, table, sep)
    except(Exception, NameError, psycopg2.Error) as error :
        print(traceback.format_exc())
        print ("Erreur lors de l'insertion des données importées dans la table variable dans le try :", error)
        con.rollback()

    ########### MODALITES ################
    con = fonctions2.connexion()
    if not fonctions2.table_exists(con, f'aa_{nom_schema}_mod'): # Si la table valeur n'existe pas dans pg, on la créer
        nom = glob.glob(path_vars+nom_csv_modalite)[0]   
        print(styleP('Création de la table modalité \n', color='purple'))
        nbclE = 0
        CleFK = []
        ClePK = 'CodeModalID'
        tableRef = []

        # attribution du fichier Variables.csv à la variable fichierCSV
        fichierCSV = pd.read_csv(nom, delimiter=';', encoding='utf-8-sig')

        fichierCSV['CodeModalEXT'] = fichierCSV['CodeModalEXT'].astype(str)
        fichierCSV['CodeModalID'] = fichierCSV['CodeModalID'].astype(int)   
        fichierCSV['Categ_regroupement'] = fichierCSV['Categ_regroupement'].astype(str)
        fichierCSV['Libelle_long'] = fichierCSV['Libelle_long'].astype(str)
        fichierCSV['LibelleModal'] = fichierCSV['LibelleModal'].astype(str)

        #fichierCSV.to_csv(nom, sep=';', encoding='utf-8-sig', index=False)

        con = fonctions2.connexion()
        fonctions2.creationTableAvecFichierComplet(fichierCSV, nom, nom_schema, con, nbclE, CleFK, ClePK, tableRef)

    print(styleP('Insertion des données dans la table modalité \n', style='bold'))
    nom = glob.glob(path_vars+'new_'+nom_csv_modalite)[0]
    try:
        con = fonctions2.connexion()
        table = nom_schema+"."+modalite.lower()                  
        sep = ';'
        fonctions2.insertionDonnees(nom, con, table, sep)
    except(Exception, NameError, psycopg2.Error) as error :
        print(traceback.format_exc())
        print ("Erreur lors de l'insertion des données importées dans la table modalité dans le try :", error)
        con.rollback()
                        
    ################ VALEURS ################
    con = fonctions2.connexion()
    if not fonctions2.table_exists(con, f'{theme}_{base}_{source}'): # Si la table valeur n'existe pas dans pg, on la créer
        csv_files, path = glob.glob('DossierFichiersTraités/*.csv')[0], glob.glob('DossierFichiersTraités/*.csv')[0]
        dfs = pd.read_csv(csv_files, sep=';', encoding='utf-8-sig')
        csv_files = csv_files.replace('DossierFichiersTraités\\','')
        fichierCSV = dfs

        fichierCSV['Annee'] = fichierCSV['Annee'].astype(int)
        fichierCSV['Date_versement'] = pd.to_datetime(fichierCSV['Date_versement'])
        fichierCSV['CodeVarID'] = fichierCSV['CodeVarID'].astype(int)
        fichierCSV['CodeModalID'] = fichierCSV['CodeModalID'].astype(int)
        fichierCSV['codeEntite'] = fichierCSV['codeEntite'].astype(int)  ### mettre en int ????
        fichierCSV['Echelle'] = fichierCSV['Echelle'].astype(str)
        fichierCSV['Valeur'] = fichierCSV['Valeur'].astype(float)
        fichierCSV['IndicePosition'] = fichierCSV['IndicePosition'].astype(str)

        ClePK = fichierCSV.columns[-3]
        nom = csv_files
        con = fonctions2.connexion()
        nbclE = 2
        tableRef = [variable, modalite]
        CleFK = [fichierCSV.columns[0], fichierCSV.columns[3]]
        fonctions2.creationTableAvecFichierComplet(fichierCSV, nom, nom_schema, con, nbclE, CleFK, ClePK, tableRef)

    print(styleP('Insertion des données dans la table valeur \n', style='bold'))
    path = glob.glob('DossierFichiersTraités/*.csv')[0]
    nom = path.replace('DossierFichiersTraités\\','')
    try:
        con = fonctions2.connexion()
        sep = ';'
        fichierCSVpath = path
        print(styleP('Insertion de données \n', style='bold'))
        table = str(nom_schema+"."+nom.replace('.csv','').lower())
        fonctions2.insertionDonnees(fichierCSVpath, con, table,sep)
    except(Exception, NameError, psycopg2.Error) as error :
        print(traceback.format_exc())
        print ("Erreur lors de l'insertion des données importées dans la table dans le try :", error)
        con.rollback()

##################################################################################################################################
### liste schema
##################################################################################################################################

def liste_schema():
    import fonctions2
    con = fonctions2.connexion()
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

def liste_table(afficher):
    import fonctions2
    con = fonctions2.connexion()
    cur = con.cursor()
    cur.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{nom_schema}'
        AND table_type = 'BASE TABLE'
    """)
    tables = cur.fetchall()
    cur.close()
    con.close()
    liste = []
    for table in tables:
        if afficher == 'vrai':
            print(table[0])
        liste.append(table[0])
    return liste


##################################################################################################################################
### existence table
##################################################################################################################################
                                                                
def table_exists(con, table_name):
    with con.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_name = %s
            );
        """, (table_name,))
        return cursor.fetchone()[0]

##################################################################################################################################
### importation table
##################################################################################################################################

def importe(table):

    import fonctions2
    import pandas as pd

    con = fonctions2.connexion()
    cur = con.cursor()

    mod_col_names = ['LibelleModal','CodeModalID','CodeModalEXT','Categ_regroupement','Libelle_long']
    var_col_names = ['CodeVarEXT','LibAxeAnalyse','Origine','CodeVarID']

    if table == 'mod':
        col_names = mod_col_names
        chemin = path_vars+nom_csv_modalite
    elif table == 'var':
        col_names = var_col_names
        chemin = path_vars+nom_csv_variable

    try:
        query = f"SELECT * FROM {nom_schema}.aa_{nom_schema}_{table}"
        cur.execute(query)
        table = pd.DataFrame(cur.fetchall(), columns=col_names)
        table.columns = col_names
        table.to_csv(chemin, index=False, sep=';', encoding='utf-8-sig')
    except Exception as e:
        print("Une erreur s'est produite :", e)
        con.rollback()  # annule la transaction en cas d'erreur

##################################################################################################################################
### liste feuilles
##################################################################################################################################
    
def liste_feuille(nom_Excel):
    import pandas as pd
    df = pd.ExcelFile(str(nom_Excel))    
    for sheet in df.sheet_names:
        print(sheet)

##################################################################################################################################
### suppression des fichiers
##################################################################################################################################

def suppression_fichiers():
    import shutil # to move files 
    doss_to_delete = ['DossierFichiersbrutesCsv','DossierFichiersTraités','vars']
    for doss in range(len(doss_to_delete)):
        try:
            shutil.rmtree(doss_to_delete[doss])
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))

# ##################################################################################################################################
# ### versement des données
# ##################################################################################################################################

# def versementDonnees(table):

#     import fonctions2
#     import glob
#     import pandas as pd

#     con = fonctions2.connexion()
#     # chemin = 
#     # table = 
#     if not fonctions2.table_exists(con, f'aa_{nom_schema}_var'):
#         nom = glob.glob(path_vars+nom_csv_variable)[0]   
#         print('Création de la table variable \n')
#         nbclE = 0
#         CleFK = []
#         ClePK = 'CodeVarID'
#         tableRef = []

#         fichierCSV = pd.read_csv(nom, delimiter=';', encoding='utf-8-sig')

#         fichierCSV['CodeVarEXT'] = fichierCSV['CodeVarEXT'].astype(str)
#         fichierCSV['CodeVarID'] = fichierCSV['CodeVarID'].astype(int)   
#         fichierCSV['LibAxeAnalyse'] = fichierCSV['LibAxeAnalyse'].astype(str)
#         fichierCSV['Origine'] = fichierCSV['Origine'].astype(str)

#         con = fonctions2.connexion()
#         fonctions2.creationTableAvecFichierComplet(fichierCSV, nom, nom_schema, con, nbclE, CleFK, ClePK, tableRef)

#     print('Insertion de données dans la table variable \n')
#     nom = glob.glob(path_vars+'new_'+nom_csv_variable)[0]
#     fichierCSVpath = nom
#     con = fonctions2.connexion()
#     table = nom_schema+"."+variable.lower()
#     sep = ';'
#     fonctions2.insertionDonnees(fichierCSVpath, con, table, sep)

##################################################################################################################################
##################################################################################################################################
########################################################## DICTIONNAIRE ##########################################################
##################################################################################################################################
##################################################################################################################################
def dico(nom_schema): #changement

    import psycopg2
    import fonctions2
    import pandas as pd

    con = fonctions2.connexion()
    cur = con.cursor()

    cur.execute(f"""
        SELECT NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = \'{nom_schema}\'
        LIMIT 1
        );

    """)

    vide = cur.fetchall()[0][0]

    if vide:
        print(styleP('Pas de données, pas de dico :( ', color='blue', style= 'bold'))
        

    else:
        # Les 2 requêtes qui vont suivre ne sont pas obligatoires,
        # c'est au cas où on décide de changer de nom aux table mod et var dans pg

        con = fonctions2.connexion()
        cur = con.cursor()
        cur.execute(f"""

            SELECT table_name

            FROM information_schema.tables

            WHERE table_schema = \'{nom_schema}\'

            AND table_name LIKE '%mod';

        """)
        mod = cur.fetchall()
        mod = mod[0][0]

        cur.execute(f"""

            SELECT table_name

            FROM information_schema.tables

            WHERE table_schema = \'{nom_schema}\'

            AND table_name LIKE '%var';

        """)

        var = cur.fetchall()
        var = var[0][0]
        liste = fonctions2.liste_table(afficher='faux')

        requete = ""

        for table in liste:

            if table != mod and table != var:

                unico = f"""select distinct {table}.codevarid, {table}.codemodalid, {table}.annee, {table}.echelle,

                        {mod}.libellemodal,{mod}.codemodalext,{mod}.categ_regroupement,

                        {mod}.libelle_long,{var}.codevarext,{var}.libaxeanalyse,{var}.origine

                        from {nom_schema}.{table}

                        join {nom_schema}.{mod} on {table}.codemodalid={mod}.codemodalid

                        join {nom_schema}.{var} on {table}.codevarid={var}.codevarid union all """

                requete += unico

        requete = requete.rstrip(' union all ')

        con = fonctions2.connexion()
        cur = con.cursor()

        col_names = ['codevarid','codemodalid','annee','echelle','libellemodal','codemodalext','categ_regroupement','libelle_long','codevarext','libaxeanalyse','origine']
        try:

            cur.execute(requete)
            table = pd.DataFrame(cur.fetchall(), columns=col_names)
            table.columns = col_names
            table
        except Exception as e:

            print("Une erreur s'est produite :", e)
            con.rollback()  # annule la transaction en cas d'erreur

        print(styleP(f"Ceci est le dictionnaire du schéma {nom_schema}", style= 'bold'))

        return table


    # # Afficher le contenu de chaque table
    # for table_name in table_names:
    #     table_name = table_name[0]
    #     query = f"SELECT * FROM socio_hab_test.{table_name};"
    #     cursor = conn.cursor()
    #     cursor.execute(query)
    #     results = cursor.fetchall()
    #     cursor.close()

    #     # Afficher les résultats
    #     print(f"Contenu de la table {table_name}:")
    #     for row in results:
    #         print(row)

    # # Fermer la connexion
    # con.close()

def dico2(nom_table): #changement
    import psycopg2
    import fonctions2
    import pandas as pd
    #print(nom_schema,nom_table)
    con = fonctions2.connexion()
    cur = con.cursor()

    cur.execute(f"""
        SELECT NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = \'{nom_schema}\'
        LIMIT 1
        );
    """)
    vide = cur.fetchall()[0][0]

    if vide:
        print('Pas de données, pas de dico :/')

    else:
        # Les 2 requêtes qui vont suivre ne sont pas obligatoires, 
        # c'est au cas où on décide de changer de nom aux table mod et var dans pg
        con = fonctions2.connexion()
        cur = con.cursor()

        cur.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = \'{nom_schema}\'
            AND table_name LIKE '%mod';
        """)
        mod = cur.fetchall()
        mod = mod[0][0]

        cur.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = \'{nom_schema}\'
            AND table_name LIKE '%var';
        """)
        var = cur.fetchall()
        var = var[0][0]

        if nom_table != mod and nom_table != var:
            requete = f"""select distinct 
                    {nom_table}.codemodalid, {mod}.libellemodal, {mod}.codemodalext, {mod}.categ_regroupement, {mod}.libelle_long,
                    {nom_table}.codevarid, {var}.codevarext,{var}.libaxeanalyse, 
                    {var}.origine, {nom_table}.annee, {nom_table}.echelle  
                    from {nom_schema}.{nom_table} 
                    join {nom_schema}.{mod} on {nom_table}.codemodalid={mod}.codemodalid 
                    join {nom_schema}.{var} on {nom_table}.codevarid={var}.codevarid"""

        con = fonctions2.connexion()
        cur = con.cursor()
        col_names = ['codevarid','codemodalid','annee','echelle','libellemodal','codemodalext','categ_regroupement','libelle_long',
                    'codevarext','libaxeanalyse','origine']
        try:
            cur.execute(requete)
            table = pd.DataFrame(cur.fetchall(), columns=col_names)
            table.columns = col_names
            table

            ##### ajout de la table créée dans pgAdmin


            ### fin 

        except Exception as e:
            print("Une erreur s'est produite :", e)
            con.rollback()  # annule la transaction en cas d'erreur

        print(f"Ceci est le dictionnaire de la table {nom_table}")



        return table


##################################################################################################################################
################################################ VUE ####################################################################
##################################################################################################################################

def creationVueSchema(base,source,theme): #changement

    """
        Donnée : nom_schema représente le nom du schéma dans lequel le jeu de donées sera versé 

        Résultat : création de deux (O2) vues:
                     * La première est la vue de reconstruction selon la structure de la table du jeu de données à verser
                     * La seconde est une réplique de la prmière vue en plus d'indicateurs à créer comme on le souhaite       
    """
    ######## IMPORT #############################
    import psycopg2
    from tqdm import tqdm
    import pandas as pd
    import fonctions2
    import glob

    ############ CREATION DE LA VUE de reconstruction de la table (obligatoire)
    print(styleP("""
    Création des vues...""", color='blue', style='bold'))

    ######## STEP 1 ::: RECUPERATION DES TABLES PRINCIPALES DANS LE SCHEMA ####################################################################################
    con = fonctions2.connexion()
  #  rep = []
    cursor = con.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = %s ",(nom_schema, theme + '_' + base + '_' + source))
    rep = cursor.fetchall()

    # Affichage des résultats
    if rep:
        print(styleP("Table trouvée :", color= 'green'))
        for row in rep:
            print(row[0])  
    else:
        print(styleP("Aucune table trouvée.", color= 'red'))

        ######## STEP 2 ::: RECUPERATION DES MODALITES A REPRESENTER PAR TABLE ####################################################################################

    for tab in tqdm(range(len(rep))):
        #nomenclature 
        view_name = rep[tab][0]
        print(view_name)
        name_avec_schema = nom_schema+"."+view_name

        table_name=   name_avec_schema
        print(table_name)

        # les modalités présentes dans la table
        sqlQuery_modalites = "select distinct {nom_schema}.{modalites}.codemodalext "+                                "from {nom_schema}.{modalites}, {name_avec_schema} "+                                "where {modalites}.codemodalid = {name_avec_schema}.codemodalid"

        sqlQuery_modalites = sqlQuery_modalites.format(nom_schema=nom_schema,name_avec_schema=name_avec_schema, table_name=table_name,
                            modalites= 'aa_{}_mod'.format(nom_schema).lower() , 
                            variables= 'aa_{}_var'.format(nom_schema).lower() 

                                                    )
        try:
            tab_modalites_avec_type = []
            tab_modalites_sans_type = []
            cursor = con.cursor()
            cursor.execute(sqlQuery_modalites)
            result = cursor.fetchall()
            for i in range(len(result)):
                # ajout du suffix "numeric" lorsque le nom de la modalité commence par '_'
                new_name = result[i][0]
                string1 = new_name[0]
                if(string1 == '_'):
                    new_name = new_name.replace(string1, 'numeric'+string1,1)

                tab_modalites_sans_type.append(str(new_name))
                tab_modalites_avec_type.append(str('"'+new_name+'"' + " double precision"))  ### changer ici si on veut numeric... plutôt que double_precision

        except (Exception, psycopg2.Error) as error :
            print(sqlQuery_modalites)

        ######## STEP 3 ::: CREATION DE VUES ####################################################################################

        sqlQuery =  """
            CREATE VIEW  {nom_schema}.V_{view_name} AS SELECT * FROM crosstab(
                ' SELECT concat({name_avec_schema}.codeentite, {name_avec_schema}.annee) as row_id, {name_avec_schema}.codeentite, 
                {name_avec_schema}.annee, {nom_schema}.{modalites}.codemodalext, {name_avec_schema}.valeur
            FROM  {name_avec_schema}, {nom_schema}.{modalites}
                WHERE {nom_schema}.{modalites}.codemodalid = {name_avec_schema}.codemodalid',
                $$ values """

        # Ajout des valeurs précises
        for i in range(len(tab_modalites_sans_type)):
            if(i == len(result)-1):
                sqlQuery = sqlQuery+ " ('{}') $$".format(tab_modalites_sans_type[i])
            else:
                sqlQuery = sqlQuery+ " ('{}'),".format(tab_modalites_sans_type[i])
        sqlQuery = sqlQuery + ') AS table_vue ("row_id" integer,"codeentite" integer, "annee" integer,'    # text

        for i in range(len(tab_modalites_avec_type)):
            if(i == len(result)-1):
                sqlQuery = sqlQuery+ " {}".format(tab_modalites_avec_type[i])
            else:
                sqlQuery = sqlQuery+ " {},".format(tab_modalites_avec_type[i])
        sqlQuery = sqlQuery +");"
        sqlQuery = sqlQuery.format(name_avec_schema=name_avec_schema, nom_schema=nom_schema, table_name=table_name,view_name=view_name,
                        modalites= 'aa_{}_mod'.format(nom_schema).lower(),
                        variables= 'aa_{}_var'.format(nom_schema).lower()
                        ) 

        try:
            cur = con.cursor()
        #    dropingLine2 = 'DROP VIEW  IF EXISTS ' + nom_schema+'.V_'+view_name+'_avec_Indicateur' + ';'
            dropingLine = 'DROP VIEW  IF EXISTS ' + nom_schema+'.V_'+view_name + ' CASCADE;'
        #    cur.execute(dropingLine2)
            cur.execute(dropingLine)
            cur.execute(sqlQuery)
            con.commit()
            print('______________________________________________________________________________')
            print(styleP("La vue " + nom_schema+'.V_'+view_name + " à été créée avec succès dans PostgreSQL \n", style='bold'))

        except (Exception) as error :
            print ("Erreur lors de la création de la vue "+ nom_schema+'.V_'+view_name +" dans PostgreSQL: ", error)
            print(sqlQuery)
    

    ########################### calcul des indicateurs        

    print(styleP(""" 
        Souhaitez-vous calculer des indicateurs à partir de la vue créée? \n
        Veuillez répondre par OUI ou NON \n """ , style= 'bold', color= 'bleu'))

    OK = False
    while( not  OK):
        reponse = input().upper()

        if(reponse == ""):
            print(""" 
        Veuillez répondre à la question par OUI ou NON \n """)               

        elif(reponse == 'NON'):
            print(styleP("""
        Opérations terminée \n
        Votre vue sans indicateurs a été créée  """, color='green'))
            OK = True

        ########## SI INDICATEURS à CREER

        elif(reponse == 'OUI'):

            print(styleP("""
        Vous pouvez ajouter des indicateurs calculés pour plus d'informations sur vos données \n """, style= 'bold'))
            OK = True

            choix_ok=False
            while(not choix_ok):
                ### liste des colonnes
                                
                table_mod = nom_schema+'.'+'aa_{}_mod'.format(nom_schema).lower()  #######   ATTENTION  #'.V_'+view_name

                info_col = get_nom_col(table_mod, con)

                if not info_col:
                    print("Aucune colonne trouvée.")
                    con.close()
                    return
                
                else: 
                    for codemodalid, libellemodal, libbelle_long in info_col:
                        print(styleP(f"Id de la colonne : {codemodalid:<6}, Nom de la colonne : {libellemodal:<19}, Libellé long : {libbelle_long:<20} \n" ,style= 'italic') )

                choix_ok=True

            nb_ind = int(input("Entrez le nombre d'indicateurs à calculer : "))

            infos_ind = []

            for i in range(nb_ind):
                print(f' Indicateur {i+1} ')

                print(""" 
        Quel type de calcul souhaitez-vous effectuer? \n 
        Veuillez entrez un numéro: \n
        1: part ... en % (modalite1/modalite2) * 100  ou [ (modalite1 + modalite2 + .... + modaliteN-1)/modaliteN ] * 100                                 
        2: somme (modalite1 + modalite2 + .... + modaliteN) \n                           
        3: différence (modalite1 - modalite2 - .... - modaliteN)                       
                                                            
                                                                 """)
                type_calcul = input()  ############ int!         1: part... en %  (modalite1/modalite2 * 100)   \n  

                colonnes = input("""
        Veuillez saisir les variables pour le calcul (séparées pas des virgules) : """).split(',')  

                nom_ind = input("""
        Choisissez un nom pour l\'indicateur à créer :""" )  
                
                #### Création de l'indicateur

                info_ind = {"type": type_calcul, "variables": colonnes, "nom_indicateur": nom_ind}
                infos_ind.append(info_ind)

            sqlQuery2 = """ CREATE VIEW  {nom_schema}.V_{view_name}_avec_Indicateur  AS SELECT *, """

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

                # if type_calcul == '1':
                #     # sqlQuery2 += f"coalesce({colonnesPart},0) * 100 AS {nom_ind}, "
                #     # rajouter ici que si l'une des colonnes est à nulle alors la valeur de la part doit être null
                #     sqlQuery2 +=   f"CASE WHEN NOT ({colonnesPart} IS NULL OR {colonnesPart} = 0) THEN {colonnesPart} * 100 ELSE NULL END AS {nom_ind}, "

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
                #    print(somme_num_col)
                #    print(denum_col)
                # sqlQuery2 += f"coalesce(({somme_num_col}) / NULLIF({denum_col}, 0) * 100, 0) AS {nom_ind}, "  

                    sqlQuery2 += f"CASE WHEN NOT ({somme_num_col} IS NULL AND {denum_col} == 0) THEN (( ({somme_num_col}) / {denum_col}) * 100) ELSE NULL END AS {nom_ind}, "    
                
            sqlQuery2= sqlQuery2[:-2]

            sqlQuery2 += ' FROM  ' + nom_schema+'.V_'+view_name + ' ; ' 

            sqlQuery2 = sqlQuery2.format(nom_schema=nom_schema,view_name=view_name) 

            print(sqlQuery2)
            #print(infos_ind)

            try:
                cur = con.cursor()
                dropingLine = 'DROP VIEW  IF EXISTS ' + nom_schema+'.V_'+view_name+'_avec_Indicateur' + ';'
                cur.execute(dropingLine)
                cur.execute(sqlQuery2)
                con.commit()
                print('______________________________________________________________________________')
                print(styleP("La vue " + nom_schema+'.V_'+view_name + " à été créée avec succès dans PostgreSQL \n", style= 'bold', color= 'cyan'))

            except (Exception) as error :
                print ("Erreur lors de la création de la vue "+ nom_schema+'.V_'+view_name+'_avec_Indicateur' +" dans PostgreSQL: ", error)
                print(sqlQuery2)

        #    choix_ok= True

            print(styleP("\nLa vue avec les indicateurs a été créée avec succès !", style= 'bold', color = 'green'))

        else:
            print(""" 
        Veuillez répondre à la question par OUI ou NON \n """)



##################################################################################################################################
################################################ Mise à jour VUE ####################################################################
##################################################################################################################################

def maj_vue(base,source,theme):

    """
        Donnée : 

        Résultat : 


    """
    ######## IMPORT #############################
    import psycopg2
    from tqdm import tqdm
    import pandas as pd
    import fonctions2
    import glob
    import fonctions2

    con = fonctions2.connexion()


    cur = con.cursor()

    cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s
    AND table_type = 'VIEW'
    """, (nom_schema,))

    vues = cur.fetchall()
    if vues:
        print(styleP("Vues trouvées :", color= 'green'))
        for row in vues:
            print(row[0])  
    else:
        print(styleP("Aucune vue trouvée.", color= 'red'))

    print(styleP(""" 
    Vous êtes sûr(e) de vouloir mettre à jour une vue ? \n
    Répondez par oui ou non!             \n """ , style= 'bold', color= 'bleu'))

    OK = False
    while( not  OK):
        reponse = input().upper()

        if(reponse == ""):
            print(""" 
        Veuillez répondre à la question par OUI ou NON \n """)               

        elif(reponse == 'NON'):
            print(styleP("""
        Opérations terminée """, color='green'))
            OK = True

        ########## SI INDICATEURS à ajouter
        elif(reponse == 'OUI'):

            print(styleP("""
        Vous pouvez ajouter des indicateurs calculés pour plus d'informations sur vos données \n """, style= 'bold'))
            
            nom_vue = input("Veuillez saisir le nom complet d'une des vue affichées précédement: \n" )
            OK = True

            choix_ok=False
            while(not choix_ok):
                ### liste des colonnes
                                
                table_mod = nom_schema+'.'+'aa_{}_mod'.format(nom_schema).lower()  #######   ATTENTION  #'.V_'+view_name

                info_col = get_nom_col(table_mod, con)

                if not info_col:
                    print("Aucune colonne trouvée.")
                    con.close()
                    # return
                
                else: 
                    for  libellemodal in info_col:  #codemodalid, , libbelle_long 
                        print(styleP(f"{libellemodal} " ,style= 'italic') )        #Id de la colonne : {codemodalid:<6}, Nom de la colonne : {libellemodal:<19}, Libellé long : {libbelle_long:<20} 

                choix_ok=True

                req = ' SELECT * FROM ' + nom_schema+'.'+nom_vue+' LIMIT 5; '
                print(req)      

                try:
                    
                    cur.execute(req)
                    con.commit()
                    print("hey")
                    # sol = cur.fetchall()


                    #   revoir affichage
                    print("Voici un apercu de la structure de la table sur PgAdmin \n")
                    
                    df = pd.read_sql_query(req, con)
                    print('hola') 

                except (Exception) as error :
                    print ("Erreur big big")

                df

    con.close()




##################################################################################################################################
### get nom des colonnes
##################################################################################################################################

def get_nom_col(table, con): #changement

    import psycopg2
    import fonctions2

    try:
        con = fonctions2.connexion()
        cursor = con.cursor()
        cursor.execute(f"SELECT {table}.codemodalid, {table}.libellemodal, {table}.libelle_long FROM {table};")
        columns_info = cursor.fetchall()
        return [ column_info[1] for column_info in columns_info]   #column_info[0], column_info[1], column_info[2]
    except (Exception, psycopg2.Error) as error:
        print("Erreur lors de la récupération des noms de colonnes : ", error)
        return []


##################################################################################################################################
### style affichage print()
##################################################################################################################################
import sys
from typing import Optional

def styleP(text: str, color: Optional[str] = None, style: Optional[str] = None) -> str:         #(text, color=None, style=None):
    """

    """
    
    # Codes ANSI pour la couleur du texte
    color_codes = {
        'black': '\033[30m',
        'red': '\033[31m',
        'green': '\033[32m',
        'yellow': '\033[33m',
        'blue': '\033[34m',
        'purple': '\033[35m',
        'cyan': '\033[36m',
        'white': '\033[37m',
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