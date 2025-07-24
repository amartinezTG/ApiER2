from rest_framework.decorators import api_view
from rest_framework.response import Response
from concurrent.futures import ThreadPoolExecutor , as_completed
from django.http import HttpResponse
from api.modelos.estaciones_despachos import EstacionDespachos
from rest_framework import status
from collections import defaultdict




@api_view(['GET', 'POST'])
def estacion_porcentaje(request):
    print("INICIANDO estacion_porcentaje")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos, est["Servidor"], est["BaseDatos"], est["Codigo"]
            ): est
            for est in estaciones
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)

@api_view(['GET', 'POST'])
def porcent_estacion_facturados_info(request):
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()

    resultados = []
    # MAX_THREADS = min(20, len(estaciones))            # <= 10 hilos
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos_facturados, est["Servidor"], est["BaseDatos"], est["Codigo"]
            ): est
            for est in estaciones
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)


@api_view(['GET', 'POST'])
def porcent_facturas_info(request):
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()

    resultados = []
    # MAX_THREADS = min(20, len(estaciones))            # <= 10 hilos
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_facturas, est["Servidor"], est["BaseDatos"], est["Codigo"]
            ): est
            for est in estaciones
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)


@api_view(['GET', 'POST'])
def estacion_despachos_porcentaje(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    estacion = request.data.get('estacion')

    if not all([from_date, until_date, estacion]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)
    print("INICIANDO estacion_despachos_porcentaje")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = estaciones if int(estacion) == 0 else [e for e in estaciones if e["Codigo"] == int(estacion)]

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)


@api_view(['GET', 'POST'])
def estacion_despachos_facturados_porcentaje(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    estacion = request.data.get('estacion')

    if not all([from_date, until_date, estacion]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)
    print("INICIANDO estacion_despachos_facturados_porcentaje")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = estaciones if int(estacion) == 0 else [e for e in estaciones if e["Codigo"] == int(estacion)]

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_despachos_facturados_sp, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)

@api_view(['GET', 'POST'])
def estacion_comparacion_series(request):
    from_date = request.data.get('from')
    until_date = request.data.get('until')
    estacion = request.data.get('estacion')

    if not all([from_date, until_date, estacion]):
        return Response({"detail": "Faltan parámetros requeridos"}, status=status.HTTP_400_BAD_REQUEST)
    print("INICIANDO estacion_comparacion_series")
    estacion_despachos = EstacionDespachos()
    estaciones = estacion_despachos.estaciones()
    estaciones_filtradas = estaciones if int(estacion) == 0 else [e for e in estaciones if e["Codigo"] == int(estacion)]

    resultados = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_est = {
            executor.submit(
                estacion_despachos.comparacion_series_sp, est["Servidor"], est["BaseDatos"], est["Codigo"], from_date, until_date
            ): est
            for est in estaciones_filtradas
        }

        resultados = []
        for future in as_completed(future_to_est):
            est = future_to_est[future]  # Aquí sí corresponde correctamente
            res = future.result()
            if res:
                resultados.append({
                    "Estacion": est["Codigo"],
                    "Servidor": est["Servidor"],
                    "BaseDatos": est["BaseDatos"],
                    "Nombre": est["Nombre"],
                    "Resultados": res
                })
    if not resultados:
        return Response({"detail": "No se encontraron resultados"}, status=status.HTTP_404_NOT_FOUND)
    return Response(resultados, status=status.HTTP_200_OK)