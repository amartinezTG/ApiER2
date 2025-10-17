# En tu modelo Volumetricos_Mensuales.py
import pyodbc
from api.db_connections import CONTROLGASTG_CONN_STR
from datetime import datetime, timedelta

class VolumetricoRow:
    """Clase para representar una fila de volumétricos"""
    def __init__(self, data_dict):
        for key, value in data_dict.items():
            setattr(self, key, value)

class VolumetricosMensuales:
    def __init__(self, conn_str: str = CONTROLGASTG_CONN_STR):
        self.conn_str = conn_str
    
    def control_volumetricos_mensuales(self, date, codgas=None):
        query = """
        SELECT 
            t1.id,
            t1.periodo,
            t1.clave_instalacion,
            t1.tipo_actividad,
            t1.formato,
            t1.file_path,
            t1.xml_contenido,
            t2.Nombre,
            t2.RFC,
            t2.Codigo
        FROM [TG].[dbo].[VolumetricosMensuales] t1
        LEFT JOIN TG.dbo.Estaciones t2 ON t1.station_id = t2.Codigo
        WHERE t1.xml_contenido IS NOT NULL 
            AND t1.periodo = CONVERT(date, ?, 23)
        """
        
        if codgas:
            print("=" * 50)
            print("Con codgas:", codgas)
            query += " AND t2.Codigo = ?"
            params = (date, codgas)
            print("Params:", params)
        else:
            print("=" * 50)
            print("Sin codgas - Trae todas las estaciones")
            params = (date,)
            print("Params:", params)
        
        query += " ORDER BY t1.station_id, t1.id"
        
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            
            # Opción A: Devolver como diccionarios (mantén como está)
            result = [dict(zip(cols, row)) for row in rows]
            
            # Opción B: Devolver como objetos (descomenta si prefieres usar row.id)
            # result = [VolumetricoRow(dict(zip(cols, row))) for row in rows]
            
            print(f"[INFO] Se encontraron {len(result)} registros")
            return result
            
        except pyodbc.Error as e:
            print(f"[ERROR] ControlGas DB error: {e}")
            return []