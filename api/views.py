from rest_framework.decorators import api_view
from rest_framework.response import Response
from concurrent.futures import ThreadPoolExecutor
from django.http import HttpResponse
from .conceptos_ingresos import (
    CONCEPTOS_INGRESOS,
    CONCEPTOS_COSTOVENTA,
    CONCEPTOS_MARGEN_UTILIDAD,
    CONCEPTOS_GASTOS_OPERACION,
    CONCEPTOS_NOMINA,
    CONCEPTOS_COSTO_SOCIAL,
    CONCEPTOS_MANTENIMIENTO
)

import pandas as pd
import time
from io import BytesIO

from .onegoal import concentrado_og
from .compact import concentrado_compact


@api_view(['POST'])
def concentrado_resultados_view(request):
    year = request.data.get('year', 2024)
    with ThreadPoolExecutor() as executor:
        future_og = executor.submit(concentrado_og, year)
        future_compact = executor.submit(concentrado_compact, year)
        resultadosOG = future_og.result()
        resultadosCompact = future_compact.result()

    todos = resultadosOG + resultadosCompact
    return Response(todos)


@api_view(['POST'])
def concentrado_anual_view(request):
    MESES = [
        'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
        'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
    ]
    year = request.data.get('year', 2024)
    t0 = time.perf_counter()
    # ----------------------------------------------------------------
    # 1) Traer datos brutos concurrentemente (concentrado_og + concentrado_compact)
    # -------------------------------------
    with ThreadPoolExecutor() as executor:
        future_og = executor.submit(concentrado_og, year)
        future_compact = executor.submit(concentrado_compact, year)
        t1 = time.perf_counter()

        resultadosOG = future_og.result()
        t2 = time.perf_counter()
        resultadosCompact = future_compact.result()
        t3 = time.perf_counter()

    todos = resultadosOG + resultadosCompact
    numeros_filas  = len(todos)
    t4 = time.perf_counter()
    # ----------------------------------------------------------------
    # 2) Convertir a DataFrame y normalizar meses a numérico
    # ----------------------------------------------------------------

    df = pd.DataFrame(todos)
    for mes in MESES:
        if mes in df.columns:
            df[mes] = pd.to_numeric(df[mes], errors='coerce')
    # ----------------------------------------------------------------
    # 3) Normalizar la columna de Concepto
    # ----------------------------------------------------------------

    df['Concepto_filtrado'] = df['Concepto'].astype(str).str.strip().str.upper()
    # ----------------------------------------------------------------
    # 4) (Opcional) Sumas por Rubro
    # ----------------------------------------------------------------
    sumas_por_rubro_mes = df.groupby('Rubro')[MESES].sum()
    sumas_por_rubro_mes_dict = sumas_por_rubro_mes.to_dict(orient='index')
    # ----------------------------------------------------------------
    # 5) Agrupar conceptos para INGRESOS y COSTO_DE_VENTA
    # ----------------------------------------------------------------

    ingresos = agrupar_conceptos_por_mes(df, CONCEPTOS_INGRESOS, MESES, 'A - INGRESOS')
    costo_venta  = agrupar_conceptos_por_mes(df, CONCEPTOS_COSTOVENTA, MESES, 'B - COSTO DE VENTA')
    t5 = time.perf_counter()
    # ----------------------------------------------------------------
    # 6) Convierto listas en diccionarios para acceso rápido
    # ----------------------------------------------------------------

    ingresos_por_concepto = { item['concepto']: item for item in ingresos }
    costos_por_concepto   = { item['concepto']: item for item in costo_venta }
    # ----------------------------------------------------------------
    # 8) Calcular margen de utilidad y medir su tiempo (t6 y t7)
    # ----------------------------------------------------------------
    t6 = time.perf_counter()

    margen_de_utilidad = []
    for nombre_margen, definicion in CONCEPTOS_MARGEN_UTILIDAD.items():
        print(f"\n=== Iniciando margen para {nombre_margen} ===")
        # Empiezo un diccionario con la estructura básica
        margin_dict = {
            'concepto': nombre_margen.upper(),
            'categoria': 'MARGEN DE UTILIDAD'
        }

        # Para cada mes: sumo los ingresos definidos y resto los costos definidos
        for mes in MESES:
            if mes != 'Enero':
                continue
            suma_ing = 0
            for c_ing in definicion['ingresos']:
                valor_ing = ingresos_por_concepto.get(c_ing, {}).get(mes, 0)
                suma_ing += valor_ing
            suma_cost = 0
            for c_cost in definicion['costo_venta']:
                valor_cost = costos_por_concepto.get(c_cost, {}).get(mes, 0)
                suma_cost += valor_cost

            margin_dict[mes] = suma_ing - suma_cost
        margen_de_utilidad.append(margin_dict)
    t7 = time.perf_counter()


    costo_venta  = agrupar_conceptos_por_mes(df, CONCEPTOS_COSTOVENTA, MESES, 'B - COSTO DE VENTA')
    gastos_operacion = agrupar_conceptos_por_mes(df,CONCEPTOS_GASTOS_OPERACION,MESES,'E - GASTOS DE OPERACION')
    nominas = agrupar_conceptos_por_mes(df, CONCEPTOS_NOMINA, MESES, 'C - NOMINA')
    costo_social = agrupar_conceptos_por_mes(df, CONCEPTOS_COSTO_SOCIAL, MESES, 'D - COSTO SOCIAL')
    mantenimiento = agrupar_conceptos_por_mes(df, CONCEPTOS_MANTENIMIENTO, MESES, 'F - MANTENIMIENTO')

    return Response({
        "numeros_filas": numeros_filas,
        "sumas_por_rubro_mes": sumas_por_rubro_mes_dict,
        "ingresos": ingresos,
        "costo_venta": costo_venta,
        "margen_de_utilidad": margen_de_utilidad,
        "gastos_operacion": gastos_operacion,
        "nominas": nominas,
        "costo_social": costo_social,
        "mantenimiento": mantenimiento
        "resultados": todos,
        "timings": {
            "lanzar_threads": t1 - t0,
            "concentrado_og": t2 - t1,
            "concentrado_compact": t3 - t2,
            "juntar_resultados": t4 - t3,
            "procesamiento_pandas": t5 - t4,
            "calculo_margen": t7 - t6,
            "total": t7 - t0
        }
    })


def agrupar_conceptos_por_mes(df, conceptos_dict, meses, rubro):
    resultados = []
    for nombre, conceptos in conceptos_dict.items():
        conceptos_normalizados = [c.upper().strip() for c in conceptos]
        filtro = df[
            (df['Rubro'] == rubro) &
            (df['Concepto_filtrado'].isin(conceptos_normalizados))
        ]
        suma_por_mes = filtro[meses].sum().to_dict()
        resultados.append({
            'concepto': nombre.upper(),
            'categoria': rubro,
            **suma_por_mes
        })
    return resultados


@api_view(['GET', 'POST'])
def exportar_ingresos_excel(request):
    MESES = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
         'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

    try:
        # Obtener el año (funciona tanto para GET como POST)
        if request.method == 'POST':
            year = request.data.get('year', 2024)
        else:  # GET
            year = int(request.GET.get('year', 2024))
        
        # Obtener los datos
        with ThreadPoolExecutor() as executor:
            future_og = executor.submit(concentrado_og, year)
            future_compact = executor.submit(concentrado_compact, year)

            resultadosOG = future_og.result()
            resultadosCompact = future_compact.result()

        # Combinar y filtrar datos
        todos = resultadosOG + resultadosCompact
        df = pd.DataFrame(todos)
        
        # Filtrar solo los ingresos
        df_ingresos = df[df['Rubro'] == 'A - INGRESOS'].copy()
        
        if df_ingresos.empty:
            return Response(
                {"error": "No se encontraron registros de ingresos para el año especificado"}, 
                status=404
            )

        # Definir columnas a exportar en el orden deseado
        cols = [
            'Empresa', 'CentroCosto', 'CatCentroCosto', 'NoCuenta', 'Rubro', 'Concepto',
            'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
            'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
        ]

        # Filtrar solo las columnas que existen en el DataFrame
        cols_disponibles = [c for c in cols if c in df_ingresos.columns]

        # Crear el archivo Excel en memoria
        output = BytesIO()
        for mes in MESES:
            if mes in df_ingresos.columns:
                df_ingresos[mes] = pd.to_numeric(df_ingresos[mes], errors='coerce')
        # Usar ExcelWriter para más control sobre el formato
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_ingresos[cols_disponibles].to_excel(
                writer, 
                sheet_name=f'Ingresos_{year}', 
                index=False
            )

            # Opcional: Ajustar el ancho de las columnas
            worksheet = writer.sheets[f'Ingresos_{year}']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Máximo 50 caracteres
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        output.seek(0)

        # Crear la respuesta HTTP
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        # Nombre del archivo con el año
        filename = f'ingresos_{year}.xlsx'
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        return response
        
    except Exception as e:
        return Response(
            {"error": f"Error al generar el archivo Excel: {str(e)}"}, 
            status=500
        )