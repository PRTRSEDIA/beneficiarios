import requests

bdns_code = 860923

#url = f"https://www.infosubvenciones.es/bdnstrans/api/convocatoria/{bdns_code}"
url = f"https://www.infosubvenciones.es/bdnstrans/GE/es/convocatorias/%d" % (bdns_code)
response = requests.get(url)
print(response.raise_for_status())

"""
if response.status_code == 200:
    data = response.json()
    salida = {
            "Código BDNS": bdns_code,
            "Título": data.get("titulo", "N/A"),
            "Órgano Convocante": data.get("organoConvocante", {}).get("nombre", "N/A"),
            "Finalidad": data.get("objeto", "N/A"),
            "Beneficiarios": data.get("beneficiarios", "N/A"),
            "Importe Estimado": data.get("importeTotalPresupuestado", "N/A"),
            "Estado": data.get("estadoConvocatoria", {}).get("descripcion", "N/A")}
else:
    salida = {"Código BDNS": bdns_code, "Error": f"No se pudo obtener información (status code {response.status_code})"}

print(salida)
"""