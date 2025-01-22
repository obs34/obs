import time

def combien_de_temps(func):
    def wrapper(*args, **kwargs):
        t_start = time.time()
        r = func(*args, **kwargs)
        t_end = time.time()
        print(f"{func.__name__ } a pris {str(round(t_end - t_start, 2))} secondes")
        return r

    return wrapper

def demander_choix_binaire(message):
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