# Guía del Usuario: Exportación y Limpieza en Grail

`grail_query_to_csv.py` exporta logs de Dynatrace Grail a CSV y también puede eliminar los mismos registros de Grail.

Use esta guía como manual operacional: qué configurar, qué ejecutar, qué esperar y cómo recuperarse con seguridad.

> **AVISO IMPORTANTE - COMUNICADO LEGAL**
>
> - Este script fue desarrollado exclusivamente para auxiliar a **{{CUSTOMER_NAME}}** en flujos de exportación y eliminación de datos en Grail.
> - Este script se proporciona **TAL CUAL**, como un **PUNTO DE PARTIDA EN MEJOR ESUERZO**, y **NO ES UN PRODUCTO DYNATRACE**.
> - Dynatrace no ofrece **NINGUNA GARANTÍA**, **NINGÚN SOPORTE OFICIAL**, ni **COMPROMISO DE VERSIONAMIENTO O MANTENIMIENTO** para este script.
> - La eliminación de registros es una capacidad existente de las APIs de Dynatrace. Este script solo demuestra un posible enfoque para control de datos en Grail, gestionado por el cliente.
> - **{{CUSTOMER_NAME}}** reconoce responsabilidad por instalación, personalización, implementación, validación, revisión de seguridad y operación continua, sin **COMPROMISO DE SOPORTE FUTURO** de Dynatrace.

---

> **AVISO DE COSTOS**
>
> - El uso de esta herramienta para consultar y eliminar datos en Dynatrace Grail **puede incurrir en costos adicionales** basados en su modelo de consumo Dynatrace.
> - Los costos se calculan en función del volumen de datos **consultados** (medido en GiB), no del volumen de datos eliminados.
> - Antes de confirmar cualquier operación de eliminación, revise el **costo estimado de la consulta** que muestra el script. Esta estimación se basa en bytes consultados y su tarifa de facturación configurada.
> - Después de que la eliminación se complete exitosamente, el script consulta `dt.system.events` para informar **costos de facturación reales** para propósitos de auditoría y comparación.
> - Configure la tarifa de facturación en su archivo `.env`: `DT_LOG_QUERY_COST_RATE_PER_GIB=<su_tarifa>` (por ejemplo: `0.25` por GiB consultado en la moneda elegida).
> - **Verifique que su tarifa de facturación coincida con su contrato Dynatrace** antes de ejecutar flujos de trabajo de eliminación.

---

## Tabla de Contenidos

1. [Qué hace esta herramienta](#1-qué-hace-esta-herramienta)
2. [Antes de comenzar](#2-antes-de-comenzar)
3. [Inicio rápido (primera ejecución recomendada)](#3-inicio-rápido-primera-ejecución-recomendada)
4. [Configuración (.env)](#4-configuración-env)
5. [Procedimientos operacionales paso a paso](#5-procedimientos-operacionales-paso-a-paso)
6. [Reglas de seguridad (lea antes de eliminar)](#6-reglas-de-seguridad-lea-antes-de-eliminar)
7. [Archivos de salida y validación](#7-archivos-de-salida-y-validación)
8. [Limpieza de largo período (múltiples días o múltiples meses)](#8-limpieza-de-largo-período-múltiples-días-o-múltiples-meses)
9. [Mensajes de consola y significado](#9-mensajes-de-consola-y-significado)
10. [Solución de problemas](#10-solución-de-problemas)
11. [Referencia de API](#11-referencia-de-api)

---

## 1. Qué hace esta herramienta

Este script tiene dos modos:

1. Modo de exportación: ejecuta una consulta DQL y escribe los registros encontrados en un CSV con timestamp.
2. Modo de limpieza: después de la exportación, envía solicitudes de eliminación definitiva en Grail para los registros correspondientes.

Uso seguro típico:

1. Ejecute primero solo la exportación.
2. Verifique el CSV.
3. Active la limpieza solo después de validar el resultado de la exportación.

---

## 2. Antes de comenzar

### 2.1 Requisitos

| Requisito | Detalles |
| --- | --- |
| Python | 3.9+ (probado en 3.13) |
| Paquete Python | `requests` |
| Alcances del token Dynatrace | `storage:logs:read` y `storage:records:delete` |
| Directorio de trabajo | Carpeta que contiene `.env` y `grail_query_to_csv.py` |

### 2.2 Configuración inicial

```text
cd /path/to/buckets
python3 -m venv .venv
.venv/bin/pip install requests
```

Siempre ejecute con `.venv/bin/python` para mantener las dependencias consistentes.

---

## 3. Inicio rápido (primera ejecución recomendada)

Siga exactamente esta secuencia para una primera ejecución segura.

### Paso 1: Cree el `.env` a partir del `env.txt`

Use el `env.txt` como template inicial y cree un archivo `.env` en la misma carpeta.

```text
cp env.txt .env
```

Luego edite el `.env` y defina `DT_ENVIRONMENT`, `DT_TOKEN`, `DT_QUERY`, `DT_FROM`, `DT_TO` y `DT_OUT`.

### Paso 2: Valide configuración y permisos

```text
./.venv/bin/python grail_query_to_csv.py --validate-config
```

### Paso 3: Ejecute solo exportación

```text
./.venv/bin/python grail_query_to_csv.py
```

### Paso 4: Verifique el CSV de salida

Confirme:

1. El archivo fue creado.
2. Los timestamps y columnas se ven correctos.
3. El volumen de registros está dentro del rango esperado.

### Paso 5: Active la limpieza solo si es necesario

Use una de las opciones:

1. `DT_CLEANUP=true` en `.env`, o
2. `--cleanup` en la línea de comandos.

### Paso 6: Ejecute la limpieza

```text
./.venv/bin/python grail_query_to_csv.py --cleanup
```

Comando opcional de seguridad antes de la limpieza:

```text
./.venv/bin/python grail_query_to_csv.py --cleanup --dry-run-delete
```

---

## 4. Configuración (.env)

Use el `env.txt` como template inicial para crear el `.env`. Todas las configuraciones se leen del `.env`. Las variables de entorno del shell sobrescriben los valores del `.env`.

### 4.1 Ejemplo completo

```text
# Requerido
DT_ENVIRONMENT=https://<tenant-id>.apps.dynatrace.com
DT_TOKEN=dt0s16.<token-value>

# Consulta de exportación (DQL)
DT_QUERY=fetch logs | filter matchesValue(id, "your-filter")

# Consulta de eliminación (DQL)
# Si no se define, el script usa DT_QUERY
DT_DELETE_QUERY=fetch logs | filter matchesValue(id, "your-filter")

# Ventana de exportación
DT_FROM=2026-03-08T00:00:00.000000000Z
DT_TO=2026-03-09T00:00:00.000000000Z

# Ventana de eliminación (opcional)
# Si se omite, el script usa DT_FROM/DT_TO
DT_DELETE_FROM=2026-03-01T00:00:00.000000000Z
DT_DELETE_TO=2026-03-09T00:00:00.000000000Z

# Nombre base del CSV de salida
DT_OUT=grail_logs.csv

# Modo de limpieza (opcional)
DT_CLEANUP=true

# Zona horaria para el nombre del archivo de salida
DT_TIMEZONE=America/Sao_Paulo

# Ajuste de validación post-eliminación
DT_DELETE_VALIDATE_RETRIES=12
DT_DELETE_VALIDATE_INTERVAL_SECONDS=10
```

### 4.2 Guía de variables

| Variable | Requerida | Propósito |
| --- | --- | --- |
| `DT_ENVIRONMENT` | Sí | URL del tenant |
| `DT_TOKEN` | Sí | Token de API |
| `DT_QUERY` | Sí | Consulta de selección para exportación |
| `DT_DELETE_QUERY` | No | Consulta de selección para limpieza |
| `DT_FROM`, `DT_TO` | Sí | Ventana de tiempo de exportación |
| `DT_DELETE_FROM`, `DT_DELETE_TO` | No | Ventana de tiempo de limpieza |
| `DT_OUT` | Sí | Nombre base del archivo CSV |
| `DT_CLEANUP` | No | Activar limpieza sin `--cleanup` |
| `DT_TIMEZONE` | No | Zona horaria usada en el nombre del archivo |
| `DT_DELETE_VALIDATE_RETRIES` | No | Intentos de verificación post-eliminación |
| `DT_DELETE_VALIDATE_INTERVAL_SECONDS` | No | Retraso entre intentos de validación |

### 4.3 Formato de timestamp

Todos los valores de tiempo deben ser RFC3339 UTC con nanosegundos:

```text
YYYY-MM-DDTHH:MM:SS.000000000Z
```

Ejemplo: `2026-03-08T00:00:00.000000000Z`

---

## 5. Procedimientos operacionales paso a paso

### 5.1 Solo exportación (línea base segura)

Use esto cuando valide una consulta o recopile datos sin eliminación.

```text
./.venv/bin/python grail_query_to_csv.py
```

Resultado esperado:

1. Se ejecuta la consulta.
2. Se crea el archivo CSV con un sufijo de timestamp.
3. Los datos remotos permanecen en Grail.

### 5.2 Exportación y limpieza (misma ventana)

Use esto cuando las ventanas de exportación y eliminación deben coincidir.

```text
./.venv/bin/python grail_query_to_csv.py --cleanup
```

Resultado esperado:

1. Se ejecuta la exportación primero.
2. El script valida condiciones de seguridad para eliminar.
3. Los registros coincidentes se eliminan en bloques de 24 horas.
4. La validación post-eliminación verifica registros restantes.

### 5.3 Exportar una ventana, limpiar una ventana diferente

Si la ventana de eliminación es más amplia que la ventana de exportación, el script mostrará una advertencia y requerirá confirmación explícita.

Use esto solo cuando sea intencional.

### 5.4 Anular valores desde CLI

```text
--environment   URL o ID del tenant
--token         Token de portador
--query         Consulta DQL de exportación
--delete-query  Consulta DQL de eliminación (no debe contener limit)
--from          Inicio de exportación
--to            Fin de exportación
--delete-from   Inicio de eliminación
--delete-to     Fin de eliminación
--out           Ruta de salida CSV
--cleanup       Activar eliminación definitiva
```

Ejemplo:

```text
./.venv/bin/python grail_query_to_csv.py \
  --from 2026-01-01T00:00:00.000000000Z \
  --to   2026-01-02T00:00:00.000000000Z
```

### 5.5 Limpiar anulaciones de shell obsoletas

Si los valores de ejecuciones anteriores aún están activos en su shell, límpielos:

```text
unset DT_FROM DT_TO DT_DELETE_FROM DT_DELETE_TO DT_QUERY DT_DELETE_QUERY
```

---

## 6. Reglas de seguridad (lea antes de eliminar)

El script aplica múltiples protecciones para reducir la pérdida accidental de datos.

### 6.1 La hora de fin de eliminación debe estar al menos 4 horas en el pasado

Si es demasiado reciente, la limpieza se omite.

### 6.2 La consulta de eliminación no debe contener `| limit`

Si se encuentra, la limpieza se omite para evitar eliminación parcial.

### 6.3 Advertencia de no coincidencia de ventana

Si la ventana de limpieza se extiende fuera de la ventana de exportación, el script muestra una advertencia y pide confirmación.

### 6.4 Verificación de existencia pre-eliminación

El script verifica si los registros aún coinciden antes de las llamadas de eliminación de API.

Si no hay registros coincidentes, la limpieza se detiene con seguridad.

### 6.5 Validación post-eliminación

Después de que todos los bloques se completen, el script vuelve a consultar con `| limit 1` hasta `DT_DELETE_VALIDATE_RETRIES` intentos.

---

## 7. Archivos de salida y validación

Cada ejecución crea un nuevo archivo CSV con timestamp.

Ejemplo de secuencia:

```text
grail_logs.csv
grail_logs_20260315_143012.csv
grail_logs_20260316_090511.csv
grail_logs_20260319_174822.csv
```

Lista de verificación de validación:

1. El archivo existe y no está vacío.
2. El número de filas es plausible.
3. Los campos/columnas coinciden con el esquema esperado.
4. El rango de tiempo en CSV coincide con la ventana seleccionada.
5. El tamaño de carga/descarga informado está dentro de los límites esperados.

Nota: objetos JSON anidados y matrices se serializan como cadenas JSON en celdas CSV.

### 7.1 Validación de integridad del paquete (SHA256 del MANIFEST)

Si recibe un paquete customer-ready con `MANIFEST.txt`, puede validar la integridad de los archivos antes de usarlo.

1. Abra el directorio del paquete donde está `MANIFEST.txt`.
2. Recalcule el SHA256 de los archivos entregados.
3. Compare cada hash calculado con el valor correspondiente en la sección `SHA256` de `MANIFEST.txt`.
4. Si algún hash es diferente, el archivo fue modificado (o dañado) después de la generación del paquete.

```text
cd customer-ready-customer-name-YYYYMMDD
shasum -a 256 RUNBOOK.md RUNBOOK_BR.md RUNBOOK_ES.md grail_query_to_csv.py env.txt
```

---

## 8. Limpieza de largo período (múltiples días o múltiples meses)

La API de eliminación acepta un máximo de 24 horas por llamada.

Para períodos largos, el script divide automáticamente en bloques de 24 horas y los ejecuta secuencialmente.

Patrón operacional recomendado:

1. Primero exporte una muestra corta (1-2 días).
2. Valide la corrección de la consulta.
3. Expanda `DT_DELETE_FROM` y `DT_DELETE_TO` al rango completo.
4. Ejecute con limpieza activada.
5. Monitoree el progreso bloque por bloque.

Si se interrumpe (`Ctrl+C`), vuelva a ejecutar el comando. Los bloques completados son seguros de repetir.

Comando de reanudar:

```text
./.venv/bin/python grail_query_to_csv.py
```

---

## 9. Mensajes de consola y significado

| Mensaje | Significado |
| --- | --- |
| `Running grail query from ... to ...` | Exportación iniciada |
| `Got N records (~X bytes payload, Y); writing CSV ...` | Registros devueltos en línea con tamaño de carga estimado |
| `No in-memory records in query result; downloading from query:download endpoint` | Descarga de transmisión de resultado grande |
| `CSV written: X bytes (Y)` | Exportación completada con tamaño final del archivo CSV |
| `Downloaded CSV to ... (X bytes, Y)` | Descarga de transmisión completada con tamaño transferido |
| `WARNING: Deletion window extends beyond export window` | Rango de eliminación más amplio que rango de exportación |
| `Will delete in N chunks of 24 hours each` | Plan de limpieza mostrado |
| `[1/3] Deleting chunk 1...` | Bloque en progreso |
| `[1/3] Chunk 1 deleted successfully.` | Bloque finalizado |
| `No matching records found - nothing to delete.` | Nada que eliminar |
| `Post-delete validation passed on attempt N` | No se encontraron registros coincidentes |
| `Remote Grail data kept (not deleted).` | Modo de limpieza no activado |

### 9.1 Ejemplos de consola completa (Conteos, Bytes y Advertencia)

Ejemplo A: registros en línea con bytes de carga estimados y tamaño CSV final.

```text
python3 grail_query_to_csv.py --cleanup
Running grail query from 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
Run #5 (previous run files already exist for this base name)
Got 2,160 records (~1,555,200 bytes payload, 1.48 MB); writing CSV grail_logs_20260320_103910.csv
CSV written: 840,506 bytes (820.81 KB)
```

Ejemplo B: ventana de exportación más pequeña que ventana de eliminación (advertencia + confirmación).

```text
python3 grail_query_to_csv.py --cleanup
Running grail query from 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
Run #5 (previous run files already exist for this base name)
Got 2,160 records (~1,555,200 bytes payload, 1.48 MB); writing CSV grail_logs_20260320_103910.csv
CSV written: 840,506 bytes (820.81 KB)

⚠️  WARNING: Deletion window extends beyond export window!
  Export window: 2026-03-02T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
  Delete window: 2026-03-01T00:00:00.000000Z to 2026-03-03T00:00:00.000000Z
  ❌ Deleting data BEFORE export start: 2026-03-01T00:00:00.000000Z < 2026-03-02T00:00:00.000000Z
  You will delete records that were never downloaded to CSV!
Proceed anyway? Type 'yes' to continue:
```

Ejemplo C: sin registros en línea; descarga CSV transmitida con número de bytes transferidos.

```text
python3 grail_query_to_csv.py
Running grail query from 2026-03-01T00:00:00.000000Z to 2026-03-02T00:00:00.000000Z
No in-memory records in query result; downloading from query:download endpoint
Downloaded CSV to grail_logs_20260320_110001.csv (145,331,002 bytes, 138.60 MB)
```

### 9.2 Lista de verificación de decisión para Advertencia: No coincidencia de ventanas de eliminación

Use este flujo cuando vea:
`WARNING: Deletion window extends beyond export window!`

1. Compare las ventanas de exportación y eliminación que se muestran en la consola.
2. Si la ventana de eliminación es más amplia que la ventana de exportación, aún no escriba `yes`.
3. Expanda la ventana de exportación (`DT_FROM`/`DT_TO`) para cubrir completamente el período de eliminación previsto, luego exporte nuevamente.
4. O reduzca la ventana de eliminación (`DT_DELETE_FROM`/`DT_DELETE_TO`) para que esté completamente dentro del período exportado.
5. Vuelva a ejecutar la exportación y confirme que el recuento de registros y el tamaño de bytes estén dentro de los límites esperados.
6. Ejecute la limpieza nuevamente y escriba `yes` solo cuando la ventana de eliminación esté completamente alineada con los datos exportados.

---

## 10. Solución de problemas

### 10.1 `ModuleNotFoundError: No module named 'requests'`

Causa: intérprete de Python incorrecto.

Solución:

```text
./.venv/bin/python grail_query_to_csv.py
```

O instale el paquete:

```text
python3 -m pip install requests
```

### 10.2 `0 records returned` pero existen registros en el bloc de notas de Grail

Verificar:

1. Ventana UTC exacta en `DT_FROM` y `DT_TO`.
2. Diferencias de conversión de zona horaria local.
3. Anulaciones de shell obsoletas.

Limpie las anulaciones si es necesario:

```text
unset DT_FROM DT_TO
```

### 10.3 `Cleanup skipped: deletion end time must be at least 4 hours in the past`

Establezca `DT_DELETE_TO` a al menos 4 horas antes de la hora actual.

### 10.4 `delete execute status 400: exceeds maximum duration`

Causa: ventana de llamada de eliminación excedió 24 horas.

Solución: use límites RFC3339 exactos con `.000000000Z`. La fragmentación del script está diseñada para reforzar esto.

### 10.5 `Transient delete status polling error (N/5)`

Causa: problema de red temporal durante el sondeo de estado.

Comportamiento: el script reintenta automáticamente hasta 5 veces.

### 10.6 La validación post-eliminación no se aprueba

La replicación de Grail puede tener retrasos.

Aumente los reintentos de validación e intervalo:

```text
DT_DELETE_VALIDATE_RETRIES=30
DT_DELETE_VALIDATE_INTERVAL_SECONDS=30
```

---

## 11. Referencia de API

Referencia Swagger:
Use su propio ID de tenant para validación y verificación: `https://YOUR_TENANT_ID.apps.dynatrace.com/platform/swagger-ui/index.html?urls.primaryName=Grail+-+Storage+Record+Deletion`

| Operación | Endpoint | Método |
| --- | --- | --- |
| Ejecutar consulta | `/platform/storage/query/v2/query:execute` | POST |
| Sondear consulta | `/platform/storage/query/v2/query:poll` | GET |
| Descargar resultado | `/platform/storage/query/v1/query:download` | GET |
| Ejecutar eliminación | `/platform/storage/record/v1/delete:execute` | POST |
| Sondear estado de eliminación | `/platform/storage/record/v1/delete:status` | POST |

Notas de API de consulta:

1. El script intenta v2 primero, luego retrocede a v1.
2. Máx registros de resultado: 50,000,000.
3. Intervalo de sondeo: 2 segundos.

Notas de API de eliminación:

1. Devuelve HTTP 202 e `taskId` inmediatamente.
2. La ventana de eliminación máxima por llamada es de 24 horas.
3. La hora de fin de eliminación debe estar al menos 4 horas en el pasado.
4. Los valores de tiempo deben usar UTC y sufijo `Z`.
