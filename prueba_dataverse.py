import pandas as pd
import requests
import msal

# =========================
# CONFIGURACIÓN
# =========================
TENANT_ID = "tu-tenant-id"
CLIENT_ID = "tu-client-id"
CLIENT_SECRET = "tu-client-secret"
RESOURCE = "https://yourorg.crm.dynamics.com"  # URL de tu entorno Dataverse
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = [f"{RESOURCE}/.default"]

# Nombre lógico de la tabla en Dataverse (ejemplo: new_tablaejemplo)
ENTITY_NAME = "tablaejemplo_angela"

# =========================
# AUTENTICACIÓN
# =========================
app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=SCOPE)
access_token = token_result["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0"
}

# =========================
# EJEMPLO DE DATAFRAME
# =========================
data = {
    "name": ["Producto A", "Producto B", "Producto C"],
    "description": ["Desc A", "Desc B", "Desc C"],
    "price": [10.5, 20.0, 30.75]
}
df = pd.DataFrame(data)

# =========================
# SUBIR DATAFRAME A DATAVERSE
# =========================
url = f"{RESOURCE}/api/data/v9.2/{ENTITY_NAME}s"

for _, row in df.iterrows():
    payload = {
        "name": row["name"],
        "description": row["description"],
        "new_price": row["price"]  # Usa el nombre lógico del campo en Dataverse
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 204:
        print(f"Registro '{row['name']}' creado correctamente.")
    else:
        print(f"Error al crear '{row['name']}': {response.status_code}, {response.text}")