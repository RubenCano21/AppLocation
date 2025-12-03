# BioSync Backend

## Instalación

1. Clona el repositorio:
   ```bash
   https://github.com/RubenCano21/AppLocation.git
   ```
2. Navega al directorio del proyecto:
   ```bash
   cd AppLocation
   ```
3. Crea un nuevo entorno virtual:
   ```bash
   python -m venv venv
   ```
4. Activa el entorno virtual:
   - Windows
   ```bash
   venv/scripts/activate
   ```
   - Linux
   ```bash
   source venv/bin/activate
   ```
5. Instala las dependencias base (backend):
   ```bash
   pip install -r requirements.txt
   ```
6. (Opcional) Instala dependencias de ML para rutas de fatiga:
   ```bash
   pip install -r requirements-ml.txt
   ```
7. Define las variables de entorno:
   ```bash
   copy .env.example .env
   ```
8. Realiza las migraciones (Si es necesario):
   ```bash
   alembic upgrade head
   ```

## Uso

Para compilar y desplegar el contrato (Opcionalmente si no existe un contrato previo ya desplegado), ejecuta:

```bash
cd blockchain
python deploy.py
```

Para iniciar el servicio, ejecuta:

```bash
uvicorn api.main:app --reload --port 8000
```
