import pandas as pd
import sqlite3

# Chemin du fichier SQLite et du fichier Excel
db_path = r"C:\Users\JadS\Desktop\IUT\SAE_MigrationNoSQL\BD\bdd"
excel_path = r"C:\Users\JadS\Desktop\IUT\SAE_MigrationNoSQL\FIchiers_Consignes\Crimes.xlsx"

# Connexion à SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Création des tables SQLite
cursor.executescript('''
DROP TABLE IF EXISTS Departement;
DROP TABLE IF EXISTS Brigade;
DROP TABLE IF EXISTS Enregistrer;
DROP TABLE IF EXISTS Crime;

-- Table des crimes
CREATE TABLE IF NOT EXISTS Crime (
   code_index VARCHAR(50) PRIMARY KEY,
   lib_index VARCHAR(255) NOT NULL
);

-- Table des départements avec les vrais noms
CREATE TABLE Departement (
   id_dep VARCHAR(50) PRIMARY KEY,
   nom_departement VARCHAR(255) NOT NULL
);

-- Table des brigades (avec référence correcte à Departement)
CREATE TABLE Brigade (
   id_brigade INTEGER PRIMARY KEY AUTOINCREMENT,
   id_dep VARCHAR(50) NOT NULL,
   lib_brigade VARCHAR(255) NOT NULL,
   type_brigade VARCHAR(10) CHECK(type_brigade IN ('PN', 'GN')) NOT NULL,
   perimetre VARCHAR(255),
   UNIQUE(id_dep, lib_brigade, type_brigade),
   FOREIGN KEY (id_dep) REFERENCES Departement(id_dep)
);I

-- Table des crimes enregistrés (référence correcte à Departement et Brigade)
CREATE TABLE Enregistrer (
   id_enregistrement INTEGER PRIMARY KEY AUTOINCREMENT,
   id_brigade INTEGER NOT NULL,
   id_dep VARCHAR(50) NOT NULL,
   id_crime VARCHAR(50) NOT NULL,
   annee VARCHAR(4) NOT NULL,
   nb_occurrences INT DEFAULT 0,
   FOREIGN KEY (id_brigade) REFERENCES Brigade(id_brigade),
   FOREIGN KEY (id_dep) REFERENCES Departement(id_dep),
   FOREIGN KEY (id_crime) REFERENCES Crime(code_index)
);
''')

print("✅ Base de données et tables créées.")

# Fonction pour nettoyer les ID de département
def nettoyer_id_dep(id_dep):
    if isinstance(id_dep, float):
        id_dep = str(int(id_dep))
    else:
        id_dep = str(id_dep)
    return id_dep.split('.')[0].zfill(2)

# Charger le fichier CSV des départements
departements_csv_path = r"C:\Users\JadS\Desktop\IUT\SAE_MigrationNoSQL\CSV_Modifies\departements-france.csv"
df_departements = pd.read_csv(departements_csv_path)

# Insérer les vrais noms des départements
for _, row in df_departements.iterrows():
    id_dep = str(row["code_departement"]).zfill(2)
    nom_departement = row["nom_departement"]
    cursor.execute("INSERT OR IGNORE INTO Departement (id_dep, nom_departement) VALUES (?, ?)",
                   (id_dep, nom_departement))

print("✅ Départements avec vrais noms insérés.")

conn.commit()

# Charger le fichier Excel
xls = pd.ExcelFile(excel_path)

# Fonction améliorée pour insérer les brigades avec CSP (PN) et CGD/GN (GN)
def inserer_brigades_avec_perimetre(df, type_brigade, annee):
    for col in range(2, df.shape[1]):
        id_dep = nettoyer_id_dep(df.columns[col])

        if type_brigade == "PN":
            lib_brigade = str(df.iloc[1, col]).strip() if pd.notna(df.iloc[1, col]) else None
            perimetre = str(df.iloc[2, col]).strip() if pd.notna(df.iloc[2, col]) else None
        elif type_brigade == "GN":
            lib_brigade = str(df.iloc[0, col]).strip() if pd.notna(df.iloc[0, col]) else None
            perimetre = None

        cursor.execute("SELECT id_dep FROM Departement WHERE id_dep = ?", (id_dep,))
        if cursor.fetchone() and lib_brigade:
            cursor.execute("INSERT OR IGNORE INTO Brigade (id_dep, lib_brigade, type_brigade, perimetre) VALUES (?, ?, ?, ?)",
                           (id_dep, lib_brigade, type_brigade, perimetre))


# Fonction pour insérer les crimes
def inserer_crimes(df):
    for row in range(2, len(df)):
        code_index = str(df.iloc[row, 0])
        lib_index = str(df.iloc[row, 1]).strip() if pd.notna(df.iloc[row, 1]) else None

        if code_index and lib_index:
            cursor.execute("INSERT OR IGNORE INTO Crime (code_index, lib_index) VALUES (?, ?)", (code_index, lib_index))

# Fonction pour insérer les enregistrements de crimes
def inserer_enregistrements(df, annee):
    for row in range(2, len(df)):
        code_index = str(df.iloc[row, 0])

        for col in range(2, df.shape[1]):
            nb_occurrences = df.iloc[row, col]
            if pd.notna(nb_occurrences) and nb_occurrences > 0:
                id_dep = nettoyer_id_dep(df.columns[col])

                cursor.execute("SELECT id_brigade FROM Brigade WHERE id_dep = ?", (id_dep,))
                brigade = cursor.fetchone()

                if brigade:
                    cursor.execute(
                        "INSERT INTO Enregistrer (id_brigade, id_dep, id_crime, annee, nb_occurrences) VALUES (?, ?, ?, ?, ?)",
                        (brigade[0], id_dep, code_index, annee, int(nb_occurrences))
                    )
                else:
                    print(f"⚠️ Aucune brigade trouvée pour id_dep {id_dep}, enregistrement ignoré.")

# Fonction pour associer les périmètres aux brigades
def associer_perimetres(df):
    perimetres_dict = {}
    for col in range(2, df.shape[1]):
        lib_brigade = str(df.iloc[1, col]).strip()
        perimetre = str(df.iloc[0, col]).strip() if pd.notna(df.iloc[0, col]) else None

        if lib_brigade and perimetre:
            perimetres_dict[lib_brigade] = perimetre

    for lib_brigade, perimetre in perimetres_dict.items():
        if perimetre:
            cursor.execute("UPDATE Brigade SET perimetre = ? WHERE lib_brigade = ?", (perimetre, lib_brigade))

# Traitement des onglets Excel
for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name)
    type_brigade = "PN" if "PN" in sheet_name else "GN"
    annee = sheet_name[-4:]

    inserer_brigades_avec_perimetre(df, type_brigade, annee)
    inserer_crimes(df)
    inserer_enregistrements(df, annee)
    associer_perimetres(df)

    print(f"✅ Traitement terminé pour l'onglet {sheet_name}.")

# Validation et fermeture propre
conn.commit()
conn.close()
print("✅ Importation terminée et base de données SQLite mise à jour.")
