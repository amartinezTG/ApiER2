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
    CONCEPTOS_MANTENIMIENTO,
    CONCEPTOS_GASTOS_FIJOS,
    RUBROS_MAPPING,
    RUBROS_STAFF_MAPPING
)
import pandas as pd
import time
import math
from io import BytesIO
import numpy as np
from .onegoal import concentrado_og
from .compact import concentrado_compact
from .controlgas import ControlGas



@api_view(['POST'])
def get_er_budget_view(request):
    meses = [
        'Enero','Febrero','Marzo','Abril','Mayo','Junio',
        'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'
    ]
    año = int(request.query_params.get('year', 2025))
    data = ControlGas().get_er_budget(año)
    df  = pd.DataFrame(data)
    df_est  = df[df['Categoria'] == 'estaciones']
    df_staff  = df[df['Categoria'] == 'staff']

    df_rubro = (df_est.groupby('Rubro', as_index=False)[meses].sum())
    df_rubro_staff = (df_staff.groupby('Rubro', as_index=False)[meses].sum())
    rubro_estaciones = df_rubro.to_dict(orient='records')
    rubro_staff = df_rubro_staff.to_dict(orient='records')

    conceptos = {
        key: (
            df_est[df_est['Rubro'] == rubro_label]
            .to_dict(orient='records')
        )
        for key, rubro_label in RUBROS_MAPPING.items()
    }
    conceptos_staff = {
        key: (
            df_staff[df_staff['Rubro'] == rubro_label]
            .to_dict(orient='records')
        )
        for key, rubro_label in RUBROS_STAFF_MAPPING.items()
    }
    return Response({
        'rubro_estaciones': rubro_estaciones,
        'rubro_staff': rubro_staff,
        **conceptos,
        **conceptos_staff,
        'data': data,
    })

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
    year = request.data.get('year')
    if not year:
        return Response({"error": "El año es requerido"}, status=400)

    resultados = obtener_resultados_anuales(year)
    numeros_filas  = len(resultados)
    df = pd.DataFrame(resultados)
    ensure_numeric(df, MESES)
    df['Concepto_filtrado'] = df['Concepto'].astype(str).str.strip().str.upper()



    sumas_por_rubro_cat_mes = agrupar_rubro_categoria(df, MESES)
    porcentajes_vs_ingresos = porcentajes_vs_ingresos_cat(sumas_por_rubro_cat_mes, MESES, 'ESTACIONES')
    porcentajes_vs_ingresos_staff = porcentajes_vs_ingresos_cat(sumas_por_rubro_cat_mes, MESES, 'STAFF')


    
    secciones_estaciones = obtener_secciones(df, sumas_por_rubro_cat_mes, MESES, 'ESTACIONES')
    secciones_staff = obtener_secciones(df, sumas_por_rubro_cat_mes, MESES, 'STAFF')



    budget = get_budget_view(year, MESES)

    return Response({
        "numeros_filas": numeros_filas,
        "sumas_por_rubro_mes": sumas_por_rubro_cat_mes,
        "resultados": resultados,
        "budget": budget,
        "porcentajes_vs_ingresos": porcentajes_vs_ingresos,
        "porcentajes_vs_ingresos_staff": porcentajes_vs_ingresos_staff,
        "secciones_estaciones": secciones_estaciones,
        "secciones_staff": secciones_staff
    })
def obtener_resultados_anuales(year):
    with ThreadPoolExecutor() as executor:
        future_og = executor.submit(concentrado_og, year)
        future_compact = executor.submit(concentrado_compact, year)
        return future_og.result() + future_compact.result()

def ensure_numeric(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

def get_budget_view(year, meses):
    data = ControlGas().get_er_budget(year)
    df  = pd.DataFrame(data)
    df_est  = df[df['Categoria'] == 'estaciones']
    df_staff  = df[df['Categoria'] == 'staff']

    df_rubro = (df_est.groupby('Rubro', as_index=False)[meses].sum())
    df_rubro_staff = (df_staff.groupby('Rubro', as_index=False)[meses].sum())
    rubro_estaciones = df_rubro.to_dict(orient='records')
    rubro_staff = df_rubro_staff.to_dict(orient='records')

    conceptos = {
        key: (
            df_est[df_est['Rubro'] == rubro_label]
            .to_dict(orient='records')
        )
        for key, rubro_label in RUBROS_MAPPING.items()
    }
    conceptos_staff = {
        key: (
            df_staff[df_staff['Rubro'] == rubro_label]
            .to_dict(orient='records')
        )
        for key, rubro_label in RUBROS_STAFF_MAPPING.items()
    }
    return {
        'rubro_estaciones': rubro_estaciones,
        'rubro_staff': rubro_staff,
        **conceptos,
        **conceptos_staff,
        'data': data,
    }


def obtener_secciones(df, sumas_por_rubro_cat_mes, MESES, cat_centro_costo):
    cat = cat_centro_costo.upper()
    return {
        f"ingresos_{cat.lower()}": agrupar_conceptos_por_mes(df, CONCEPTOS_INGRESOS, MESES, 'A - INGRESOS', sumas_por_rubro_cat_mes, cat),
        f"costo_venta_{cat.lower()}": agrupar_conceptos_por_mes(df, CONCEPTOS_COSTOVENTA, MESES, 'B - COSTO DE VENTA', sumas_por_rubro_cat_mes, cat),
        f"gastos_operacion_{cat.lower()}": agrupar_conceptos_por_mes(df, CONCEPTOS_GASTOS_OPERACION, MESES, 'E - GASTOS DE OPERACION', sumas_por_rubro_cat_mes, cat),
        f"nomina_{cat.lower()}": agrupar_conceptos_por_mes(df, CONCEPTOS_NOMINA, MESES, 'C - NOMINA', sumas_por_rubro_cat_mes, cat),
        f"costo_social_{cat.lower()}": agrupar_conceptos_por_mes(df, CONCEPTOS_COSTO_SOCIAL, MESES, 'D - COSTO SOCIAL', sumas_por_rubro_cat_mes, cat),
        f"mantenimiento_{cat.lower()}": agrupar_conceptos_por_mes(df, CONCEPTOS_MANTENIMIENTO, MESES, 'F - MANTENIMIENTO', sumas_por_rubro_cat_mes, cat),
        f"gastos_fijos_{cat.lower()}": agrupar_conceptos_por_mes(df, CONCEPTOS_GASTOS_FIJOS, MESES, 'H - GASTOS FIJOS', sumas_por_rubro_cat_mes, cat),
    }
def agrupar_conceptos_por_mes(df, conceptos_dict, meses, rubro, sumas_por_rubro_cat_mes_dict, cat_centro_costo='ESTACIONES'):
    resultados = []
    ingresos = sumas_por_rubro_cat_mes_dict.get('A - INGRESOS', {}).get("ESTACIONES", {})

    for nombre, conceptos in conceptos_dict.items():
        conceptos_normalizados = [c.upper().strip() for c in conceptos]
        filtro = df[
            (df['Rubro'] == rubro) &
            (df['Concepto_filtrado'].isin(conceptos_normalizados)) &
            ((df['CatCentroCosto'] == cat_centro_costo) | (df['CatCentroCosto'] == cat_centro_costo.upper()))
        ]
        suma_por_mes = filtro[meses].sum()
        fila = {
            'concepto': nombre.upper(),
            'categoria': rubro
        }
        for mes in meses:
            total = suma_por_mes.get(mes, 0)
            suma_ingreso_mes = ingresos.get(mes, 0)
            porcentaje = (total / suma_ingreso_mes * 100) if suma_ingreso_mes else 0
            fila[mes] = {
                'total': round(total, 2),
                'porcentaje': f"{porcentaje:.2f}%"
            }
        resultados.append(fila)
    return resultados

def calcular_margen_utilidad(ingresos_por_concepto, costos_por_concepto, definiciones_margen, meses, sumas_por_rubro_mes):
    resultado = []
    for nombre_margen, definicion in definiciones_margen.items():
        margin_dict = {
            'concepto': nombre_margen.upper(),
            'categoria': 'MARGEN DE UTILIDAD'
        }

        for mes in meses:
            suma_ing = sum(
                ingresos_por_concepto.get(c_ing, {}).get(mes, {}).get('total', 0)
                for c_ing in definicion.get('ingresos', [])
            )
            suma_cost = sum(
                costos_por_concepto.get(c_cost, {}).get(mes, {}).get('total', 0)
                for c_cost in definicion.get('costo_venta', [])
            )
            utilidad = round(suma_ing + suma_cost, 2)
            total_ingreso = sumas_por_rubro_mes.get('A - INGRESOS', {}).get(mes, 0)
            porcentaje_utilidad = (utilidad / total_ingreso * 100) if total_ingreso else 0
            # print(f"Procesando {nombre_margen} para el mes {mes}:")
            # print(f"Suma Ingresos: {suma_ing}, Suma Costo: {suma_cost}, Utilidad: {utilidad}")
            # print(f"Mes: {mes}, total_ingreso: {total_ingreso}, Utilidad: {utilidad}, Porcentaje: {porcentaje_utilidad:.2f}%")
            margin_dict[mes] = {
                'total': utilidad,
                'porcentaje': f"{porcentaje_utilidad:.2f}%"
            }
            break
        resultado.append(margin_dict)
    return resultado
def limpiar_nans_dict(d):
    for k1, v1 in d.items():
        for k2, v2 in v1.items():
            if isinstance(v2, float) and math.isnan(v2):
                d[k1][k2] = 0.0
    return d

def porcentajes_vs_ingresos_cat(sumas_por_rubro_cat_mes_dict, meses, categoria_centro_costo='ESTACIONES'):
    # Asegúrate de upper() para uniformidad en las llaves
    categoria_centro_costo = categoria_centro_costo.upper()
    # Obtén el total de ingresos de esa categoría
    ingresos = sumas_por_rubro_cat_mes_dict.get('A - INGRESOS', {}).get(categoria_centro_costo, {})
    porcentajes = {}
    for rubro, cats in sumas_por_rubro_cat_mes_dict.items():
        cat_dict = cats.get(categoria_centro_costo, {})
        porcentajes[rubro] = {}
        for mes in meses:
            suma_rubro_mes = cat_dict.get(mes, 0)
            suma_ingreso_mes = ingresos.get(mes, 0)
            porcentaje = (suma_rubro_mes / suma_ingreso_mes * 100) if suma_ingreso_mes else 0.0
            porcentajes[rubro][mes] = round(porcentaje, 2)
    return porcentajes

def agrupar_rubro_categoria(df, meses):
    sumas = df.groupby(['Rubro', 'CatCentroCosto'])[meses].sum()
    resultado = {}
    for (rubro, cat), row in sumas.iterrows():
        resultado.setdefault(rubro, {})[cat] = row.to_dict()
    return resultado
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