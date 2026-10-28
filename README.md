# QueueKit

A durable SQLite job queue with leases, retries, priorities and bounded worker concurrency.

[![CI](https://github.com/MIGUEL-MOSCOSO-DEV/queuekit/actions/workflows/ci.yml/badge.svg)](https://github.com/MIGUEL-MOSCOSO-DEV/queuekit/actions/workflows/ci.yml) · [Descargas](https://github.com/MIGUEL-MOSCOSO-DEV/queuekit/releases/latest) · [MIT](LICENSE)

Un motor local de trabajos persistentes. SQLite conserva el estado y los workers reclaman tareas con leases para recuperarlas después de una interrupción.

## Qué incluye

- Prioridades, retrasos, idempotencia y hasta diez intentos por trabajo.
- Reintentos con espera exponencial y recuperación de leases vencidos.
- Concurrencia de 1 a 16 hilos, renovación del lease y protección contra resultados de workers antiguos.
- Comandos para encolar, consultar, cancelar y ejecutar trabajos; handlers incorporados para huellas SHA-256 y tamaño de carpetas.

## Descargar para Windows

En [Releases](https://github.com/MIGUEL-MOSCOSO-DEV/queuekit/releases/latest) encontrarás QueueKit.exe y su archivo SHA256SUMS.txt. Es una aplicación portable para Windows 10/11 de 64 bits: no necesita instalar Python ni permisos de administrador. Ejecuta el programa desde tu terminal; --help muestra los comandos disponibles. Los ejecutables no tienen firma digital.

## Ejecutar desde el código

Necesitas Python 3.11 o superior. Crea un entorno virtual para mantener las dependencias separadas.

```sh
git clone https://github.com/MIGUEL-MOSCOSO-DEV/queuekit.git
cd queuekit
python -m venv .venv
```

En Windows:

```powershell
.venv\Scripts\python.exe -m pip install .
.venv\Scripts\python.exe launch.py --help
```

En Linux o macOS puedes usar .venv/bin/python. Las pruebas de la lógica se ejecutan en Windows y Linux.

## Uso

queuekit enqueue TIPO --payload ARCHIVO_JSON crea un trabajo. El archivo debe contener los datos que requiere su handler; sha256 y directory-size necesitan path con una ruta local. queuekit work --concurrency 2 mantiene workers activos y queuekit work --once procesa como máximo un trabajo disponible. Usa queuekit list, queuekit show ID o queuekit cancel ID para administrar la cola.

- --db RUTA va antes del subcomando. Por defecto utiliza la base de datos del perfil.
- enqueue acepta --priority entre -100 y 100, --attempts de 1 a 10, --delay de hasta 604800 segundos y --key para idempotencia.
- Una misma clave devuelve el trabajo anterior si tipo y payload coinciden; si difieren, se rechaza.
- Desde la instalación Python, --handlers MODULO importa un módulo de confianza que exporte HANDLERS, un diccionario de funciones. Cada función recibe payload y context; debe devolver un resultado JSON y llamar a context.check_cancelled() durante operaciones largas. El ejecutable portable incluye los handlers incorporados.

## Datos y privacidad

La instalación comienza sin registros de demostración. Los datos locales se guardan en ~/.queuekit (la carpeta de tu usuario). Puedes cambiar esa ubicación con la variable QUEUEKIT_HOME. inventory.sqlite conserva catálogo, ventas y movimientos. No protege esos datos con una contraseña: la privacidad depende de tu cuenta y del disco del sistema.

## Alcance actual

Semántica de ejecución al menos una vez, no exactamente una vez: tus handlers deben ser idempotentes si producen efectos externos. Payload y resultado tienen un límite de 256 KB. Los leases duran 30 segundos con renovación periódica. La cancelación es cooperativa y no revierte efectos ya producidos; no hay terminación forzada de hilos. El apagado espera a que los handlers terminen o cooperen. Usa almacenamiento local, no una carpeta de red para SQLite. No incluye panel web ni servicio de Windows instalado automáticamente.

## Desarrollo y pruebas

```sh
python -m pip install . -r requirements-dev.txt
python -m ruff check .
python -m ruff format --check .
python -m unittest discover -s tests -v
```

Las pruebas usan directorios y datos temporales. GitHub Actions verifica formato y pruebas en Windows y Linux y construye un ejecutable de Windows como artefacto del workflow. No hay una aplicación web ni un servidor remoto requerido.

## Generar el ejecutable

Desde Windows, con las dependencias de desarrollo instaladas:

```sh
python -m PyInstaller --noconfirm --clean --onefile --name QueueKit launch.py
```

El resultado queda en dist/QueueKit.exe. PyInstaller construye para el sistema donde se ejecuta; para generar Windows necesitas construir desde Windows. Los archivos de compilación y los datos locales se excluyen de Git.

## Estructura

store.py contiene la cola transaccional y tokens de lease; worker.py administra renovación, concurrencia y handlers; cli.py expone los comandos.

## Contribuir

Abre un issue con el sistema operativo, la versión y los pasos para reproducir el problema. No incluyas archivos privados, contraseñas ni bases reales. Para cambiar reglas de negocio, añade una prueba que describa el caso.

Creado por [Miguel Moscoso](https://github.com/MIGUEL-MOSCOSO-DEV). Licencia MIT.
