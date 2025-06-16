import pyodbc
from .db_connections import COMPACT_CONN_STR

class Compact:
    def __init__(self):
        self.conn = pyodbc.connect(COMPACT_CONN_STR)
        self.cursor = self.conn.cursor()

    def get_bases_empresas(self):
        query = """
        SELECT [Id], [Nombre], [AliasBDD] FROM [GeneralesSQL].[dbo].[ListaEmpresas]
        """
        rows = self.cursor.execute(query).fetchall()
        bases = []
        for row in rows:
            bases.append({
                'id': row.Id,
                'nombre': row.Nombre,
                'db': row.AliasBDD,
            })
        return bases

    def get_edo_resultados_query(self, base, anio, nombre):
        # Generar CatCentroCosto dinámico igual que en PHP
        CatCentroCosto = ""
        if base == 'ctSERVICIO_SYC_SA_DE_CV':
            CatCentroCosto = """
                case 
                    when t3.id in (8,5) then 'STAFF'
                    when t3.id = 12 then 'ESTACIONES'
                    ELSE 'n/a'
                END AS [CatCentroCosto],
            """
        elif base == 'ctSERVICIOS_GASOLINEROS_EL_CASTANO_SA':
            CatCentroCosto = """
                case 
                    when t3.id in (8,5,13,16) then 'STAFF'
                    when t3.id in (33,1,65) then 'ESTACIONES'
                    when t3.id = 6 then 'CORPORATIVO'
                    ELSE 'n/a'
                END AS [CatCentroCosto],
            """
        elif base == 'ctGASOLINERA_VILLA_AHUMADA':
            CatCentroCosto = """
                case 
                    when t3.id in (9,13) then 'STAFF'
                    when t3.id in (5,17) then 'ESTACIONES'
                    when t3.id = 21 then 'CORPORATIVO''
                    ELSE 'n/a'
                END AS [CatCentroCosto],
            """

        query = f"""
        SELECT
            '{nombre}' As Empresa,
            t3.Nombre AS [CentroCosto],
            {CatCentroCosto}
            TRY_CAST(t2.Codigo AS INT) AS [NoCuenta],
            CASE
                WHEN t2.DigAgrup = 1 THEN 'A - INGRESOS'
                WHEN t2.DigAgrup = 2 THEN 'B - Costo de Venta'
                WHEN t2.DigAgrup = 3 THEN 'C - Nómina'
                WHEN t2.DigAgrup = 4 THEN 'D - Costo Social'
                WHEN t2.DigAgrup = 5 THEN 'E - Gasto Operación'
                WHEN t2.DigAgrup = 6 THEN 'F - Mantenimiento'
                WHEN t2.DigAgrup = 8 THEN 'H - Gastos Fijos'
                WHEN t2.DigAgrup = 9 THEN 'I - Ingresos no operativo'
                WHEN t2.DigAgrup = 11 THEN 'K - CIF'
                ELSE 'Otros'
            END AS Rubro,
            t2.Nombre AS [Concepto],
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe1  - t1.SaldoIni) ELSE -(t1.Importe1  - t1.SaldoIni) END AS Enero,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe2  - t1.Importe1) ELSE -(t1.Importe2  - t1.Importe1) END AS Febrero,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe3  - t1.Importe2) ELSE -(t1.Importe3  - t1.Importe2) END AS Marzo,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe4  - t1.Importe3) ELSE -(t1.Importe4  - t1.Importe3) END AS Abril,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe5  - t1.Importe4) ELSE -(t1.Importe5  - t1.Importe4) END AS Mayo,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe6  - t1.Importe5) ELSE -(t1.Importe6  - t1.Importe5) END AS Junio,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe7  - t1.Importe6) ELSE -(t1.Importe7  - t1.Importe6) END AS Julio,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe8  - t1.Importe7) ELSE -(t1.Importe8  - t1.Importe7) END AS Agosto,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe9  - t1.Importe8) ELSE -(t1.Importe9  - t1.Importe8) END AS Septiembre,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe10 - t1.Importe9) ELSE -(t1.Importe10 - t1.Importe9) END AS Octubre,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe11 - t1.Importe10) ELSE -(t1.Importe11 - t1.Importe10) END AS Noviembre,
            CASE WHEN t2.Tipo = 'H' THEN (t1.Importe12 - t1.Importe11) ELSE -(t1.Importe12 - t1.Importe11) END AS Diciembre
        FROM [{base}].dbo.SaldosSegmentoNegocio AS t1
        LEFT JOIN [{base}].dbo.Cuentas AS t2 ON t1.IdCuenta  = t2.ID
        LEFT JOIN [{base}].dbo.SegmentosNegocio AS t3 ON t1.IdSegNeg  = t3.Id
        LEFT JOIN [{base}].dbo.ejercicios as t4 on t1.Ejercicio = t4.Id
        LEFT JOIN [{base}].dbo.AgrupadoresSat as t5 on t2.IdAgrupadorSAT = t5.Id
        WHERE 
            t4.FecIniEje = '{anio}-01-01 00:00:00.000'
            AND t1.Tipo      = 1
            AND t2.DigAgrup    <> 0
            AND t1.IdSegNeg > 0
            AND t2.SegNegMovtos = 1
        ORDER BY t2.DigAgrup, t1.IdCuenta
        """
        try:
            rows = self.cursor.execute(query).fetchall()
            cols = [column[0] for column in self.cursor.description]
            return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            print(f"Error: {e}")
            return []
        
# --------- AGREGA ESTA PARTE AL FINAL DEL ARCHIVO ---------
# Esta función se encargará de concentrar los resultados de las empresas
# def concentrado_compact(year):
#     compact = Compact()
#     empresas = compact.get_bases_empresas()
#     resultados = []
#     for empresa in empresas:
#         if empresa['db'] not in [
#             'ctSERVICIO_SYC_SA_DE_CV',
#             'ctSERVICIOS_GASOLINEROS_EL_CASTANO_SA',
#             'ctGASOLINERA_VILLA_AHUMADA'
#         ]:
#             continue
#         base = empresa['db']
#         nombre = empresa['nombre']
#         edo = compact.get_edo_resultados_query(base, year, nombre)
#         resultados.extend(edo)
#     return resultados
def concentrado_compact(year):
    compact = Compact()
    bases = [
        ('ctSERVICIO_SYC_SA_DE_CV', "'SERVICIO SYC S.A. DE C.V.' As Empresa, t3.Nombre as [CentroCosto], case when t3.id in (8,5) then 'STAFF' when t3.id = 12 then 'ESTACIONES' ELSE 'n/a' END AS [CatCentroCosto],"),
        ('ctSERVICIOS_GASOLINEROS_EL_CASTANO_SA', "'SERVICIOS GASOLINEROS EL CASTAÑO S.A.' As Empresa, t3.Nombre as [CentroCosto], case when t3.id in (8,5,13,16) then 'STAFF' when t3.id in (33,1,65) then 'ESTACIONES' when t3.id = 6 then 'Corporativo' ELSE 'n/a' END AS [CatCentroCosto],"),
        ('ctGASOLINERA_VILLA_AHUMADA', "'GASOLINERA VILLA AHUMADA' As Empresa, t3.Nombre as [CentroCosto], case when t3.id in (9,13) then 'STAFF' when t3.id in (5,17) then 'ESTACIONES' when t3.id = 21 then 'Corporativo' ELSE 'n/a' END AS [CatCentroCosto],")
    ]

    queries = []
    for base, catCentroCosto in bases:
        queries.append(f"""
            SELECT
                {catCentroCosto}
                TRY_CAST(t2.Codigo AS INT) AS [NoCuenta],
                CASE
                    WHEN t2.DigAgrup = 1 THEN 'A - INGRESOS'
                    WHEN t2.DigAgrup = 2 THEN 'B - COSTO DE VENTA'
                    WHEN t2.DigAgrup = 3 THEN 'C - NOMINA'
                    WHEN t2.DigAgrup = 4 THEN 'D - COSTO SOCIAL'
                    WHEN t2.DigAgrup = 5 THEN 'E - GASTOS DE OPERACION'
                    WHEN t2.DigAgrup = 6 THEN 'F - MANTENIMIENTO'
                    WHEN t2.DigAgrup = 8 THEN 'H - GASTOS FIJOS'
                    WHEN t2.DigAgrup = 9 THEN 'I - INGRESOS NO OPERATIVOS'
                    WHEN t2.DigAgrup = 11 THEN 'K - CIF'
                    ELSE 'Otros'
                END AS Rubro,
                UPPER(t2.Nombre) AS [Concepto],
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe1  - t1.SaldoIni) ELSE -(t1.Importe1  - t1.SaldoIni) END AS Enero,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe2  - t1.Importe1) ELSE -(t1.Importe2  - t1.Importe1) END AS Febrero,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe3  - t1.Importe2) ELSE -(t1.Importe3  - t1.Importe2) END AS Marzo,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe4  - t1.Importe3) ELSE -(t1.Importe4  - t1.Importe3) END AS Abril,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe5  - t1.Importe4) ELSE -(t1.Importe5  - t1.Importe4) END AS Mayo,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe6  - t1.Importe5) ELSE -(t1.Importe6  - t1.Importe5) END AS Junio,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe7  - t1.Importe6) ELSE -(t1.Importe7  - t1.Importe6) END AS Julio,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe8  - t1.Importe7) ELSE -(t1.Importe8  - t1.Importe7) END AS Agosto,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe9  - t1.Importe8) ELSE -(t1.Importe9  - t1.Importe8) END AS Septiembre,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe10 - t1.Importe9) ELSE -(t1.Importe10 - t1.Importe9) END AS Octubre,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe11 - t1.Importe10) ELSE -(t1.Importe11 - t1.Importe10) END AS Noviembre,
                CASE WHEN t2.Tipo = 'H' THEN (t1.Importe12 - t1.Importe11) ELSE -(t1.Importe12 - t1.Importe11) END AS Diciembre
            FROM [{base}].dbo.SaldosSegmentoNegocio AS t1
            LEFT JOIN [{base}].dbo.Cuentas AS t2 ON t1.IdCuenta  = t2.ID
            LEFT JOIN [{base}].dbo.SegmentosNegocio AS t3 ON t1.IdSegNeg  = t3.Id
            LEFT JOIN [{base}].dbo.ejercicios as t4 on t1.Ejercicio = t4.Id
            LEFT JOIN [{base}].dbo.AgrupadoresSat as t5 on t2.IdAgrupadorSAT = t5.Id
            WHERE 
                t4.FecIniEje = '{year}-01-01 00:00:00.000'
                AND t1.Tipo      = 1
                AND t2.DigAgrup    <> 0
                AND t1.IdSegNeg > 0
                AND t2.SegNegMovtos = 1
        """)
    full_query = " UNION ALL ".join(queries) + " ORDER BY Rubro, NoCuenta"

    try:
        cursor = compact.conn.cursor()
        rows = cursor.execute(full_query).fetchall()
        cols = [column[0] for column in cursor.description]
        result= [dict(zip(cols, row)) for row in rows]
        for r in result:
            if r['NoCuenta'] == 40302001 or r['NoCuenta'] == 40122000 or r['NoCuenta'] == 40101007:
                r['Rubro'] = 'I - INGRESOS NO OPERATIVOS'

        return result

    except Exception as e:
        print(f"Error: {e}")
        return []