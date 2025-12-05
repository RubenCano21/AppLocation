"""
Script para iniciar el scheduler ETL automático
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Verificar que se use Python 3.11
import platform
print(f"Python version: {platform.python_version()}")

from app.scheduler.etl_scheduler import start_scheduler

if __name__ == "__main__":
    # Ejecutar ETL cada 30 minutos por defecto
    # Cambiar el valor para ajustar la frecuencia

    print("\n" + "="*70)
    print("ETL SCHEDULER - MODO INCREMENTAL")
    print("="*70)
    print("\nConfiguración:")
    print("  • Modo: Incremental (solo datos nuevos)")
    print("  • Intervalo: 30 minutos")
    print("  • Sin eliminación de datos")
    print("\nPara cambiar el intervalo:")
    print("  python start_scheduler.py --interval 60  (cada hora)")
    print("="*70 + "\n")

    start_scheduler(interval_minutes=30)

