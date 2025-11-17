
from bdns_fetch import BDNSClient

# Inicializa el cliente
client = BDNSClient()

# ID del registro que quieres consultar
registro_id = "2024-000123"  # Sustituye por el ID real

# Llamada al endpoint para obtener el detalle del registro
detalle = client.concesiones_detalle(registro_id)

# Mostrar el resultado
print(detalle)
