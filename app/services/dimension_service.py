# app/services/dimension_service.py
from sqlalchemy.orm import Session
from app.models.db_models import (
    DimTime, DimAltitude, DimBattery,
    DimNetwork, DimOperator, DimDevice
)
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class DimensionService:
    """Servicio para poblar y gestionar tablas de dimensión"""

    @staticmethod
    def populate_dim_time(db: Session):
        """Poblar dimensión de tiempo (24 horas)"""
        try:
            # Verificar si ya existe data
            count = db.query(DimTime).count()
            if count > 0:
                logger.info(f"✓ dim_time already populated with {count} records")
                return

            logger.info("Populating dim_time...")

            time_periods = []
            for hour in range(24):
                if 6 <= hour < 12:
                    period = "MAÑANA"
                elif 12 <= hour < 19:
                    period = "TARDE"
                else:
                    period = "NOCHE"

                time_periods.append(DimTime(
                    id=hour,
                    period=period,
                ))

            db.bulk_save_objects(time_periods)
            db.commit()
            logger.info(f"✓ dim_time populated with {len(time_periods)} records")

        except Exception as e:
            db.rollback()
            logger.error(f"✗ Error populating dim_time: {e}")
            raise

    @staticmethod
    def populate_dim_altitude(db: Session):
        """Poblar dimensión de altitud"""
        try:
            count = db.query(DimAltitude).count()
            if count > 0:
                logger.info(f"✓ dim_altitude already populated with {count} records")
                return

            logger.info("Populating dim_altitude...")

            altitude_ranges = [
                (0, 400, "BAJA"),
                (400, 500, "MEDIA"),
                (500, 1000, "ALTA"),
            ]

            altitudes = []
            for i, ( range_name) in enumerate(altitude_ranges, 1):
                altitudes.append(DimAltitude(
                    id=i,
                    altitude_range=range_name,
                ))

            db.bulk_save_objects(altitudes)
            db.commit()
            logger.info(f"✓ dim_altitude populated with {len(altitudes)} records")

        except Exception as e:
            db.rollback()
            logger.error(f"✗ Error populating dim_altitude: {e}")
            raise

    @staticmethod
    def populate_dim_battery(db: Session):
        """Poblar dimensión de batería"""
        try:
            count = db.query(DimBattery).count()
            if count > 0:
                logger.info(f"✓ dim_battery already populated with {count} records")
                return

            logger.info("Populating dim_battery...")

            battery_levels = [
                (0, 25, "CRITICO"),
                (25, 50, "BAJO"),
                (50, 75, "MEDIO"),
                (75, 100, "ALTO"),
            ]

            batteries = []
            for i, ( level) in enumerate(battery_levels, 1):
                batteries.append(DimBattery(
                    id=i,
                    battery_level=level,
                ))

            db.bulk_save_objects(batteries)
            db.commit()
            logger.info(f"✓ dim_battery populated with {len(batteries)} records")

        except Exception as e:
            db.rollback()
            logger.error(f"✗ Error populating dim_battery: {e}")
            raise

    @staticmethod
    def populate_dim_network_from_data(db: Session):
        """Poblar dimensión de red desde datos existentes"""
        try:
            logger.info("Populating dim_network from existing data...")

            # Obtener tipos de red únicos de la tabla locations
            result = db.execute(text("""
                                     SELECT DISTINCT network_type,
                                                     network_type_normalized
                                     FROM locations
                                     WHERE network_type IS NOT NULL
                                     """))

            networks = []
            network_id = 1
            seen_types = set()

            for row in result:
                network_type = row[0]
                network_normalized = row[1]

                if network_type in seen_types:
                    continue
                seen_types.add(network_type)

                # Determinar generación
                generation = None
                if '5G' in network_normalized or network_normalized == '5G':
                    generation = 5
                elif '4G' in network_normalized or network_normalized == '4G':
                    generation = 4
                elif '3G' in network_normalized or network_normalized == '3G':
                    generation = 3
                elif '2G' in network_normalized or network_normalized == '2G':
                    generation = 2

                networks.append(DimNetwork(
                    id=network_id,
                    network_normalized=network_normalized,
                ))
                network_id += 1

            if networks:
                db.bulk_save_objects(networks)
                db.commit()
                logger.info(f"✓ dim_network populated with {len(networks)} records")
            else:
                logger.warning("⚠ No network data found in locations table")

        except Exception as e:
            db.rollback()
            logger.error(f"✗ Error populating dim_network: {e}")
            raise

    @staticmethod
    def populate_dim_operator_from_data(db: Session):
        """Poblar dimensión de operador desde datos existentes"""
        try:
            logger.info("Populating dim_operator from existing data...")

            result = db.execute(text("""
                                     SELECT DISTINCT sim_operator
                                     FROM locations
                                     WHERE sim_operator IS NOT NULL
                                     ORDER BY sim_operator
                                     """))

            operators = []
            operator_id = 1

            for row in result:
                operator_name = row[0]

                # Generar código (primeras 3 letras en mayúscula)
                operator_code = operator_name[:3].upper() if operator_name else "UNK"

                operators.append(DimOperator(
                    id=operator_id,
                    operator_name=operator_name,
                ))
                operator_id += 1

            if operators:
                db.bulk_save_objects(operators)
                db.commit()
                logger.info(f"✓ dim_operator populated with {len(operators)} records")
            else:
                logger.warning("⚠ No operator data found in locations table")

        except Exception as e:
            db.rollback()
            logger.error(f"✗ Error populating dim_operator: {e}")
            raise

    @staticmethod
    def populate_dim_device_from_data(db: Session):
        """Poblar dimensión de dispositivo desde datos existentes"""
        try:
            logger.info("Populating dim_device from existing data...")

            result = db.execute(text("""
                                     SELECT device_id,
                                            device_name,
                                            MIN(timestamp) as first_seen,
                                            MAX(timestamp) as last_seen
                                     FROM locations
                                     WHERE device_id IS NOT NULL
                                     GROUP BY device_id, device_name
                                     ORDER BY device_id
                                     """))

            devices = []
            device_pk_id = 1

            for row in result:
                devices.append(DimDevice(
                    id=device_pk_id,
                    device_id=row[0],
                    device_name=row[1],
                ))
                device_pk_id += 1

            if devices:
                db.bulk_save_objects(devices)
                db.commit()
                logger.info(f"✓ dim_device populated with {len(devices)} records")
            else:
                logger.warning("⚠ No device data found in locations table")

        except Exception as e:
            db.rollback()
            logger.error(f"✗ Error populating dim_device: {e}")
            raise

    @staticmethod
    def populate_all_dimensions(db: Session):
        """Poblar todas las dimensiones"""
        logger.info("=" * 70)
        logger.info("POPULATING ALL DIMENSIONS")
        logger.info("=" * 70)

        DimensionService.populate_dim_time(db)
        DimensionService.populate_dim_altitude(db)
        DimensionService.populate_dim_battery(db)
        DimensionService.populate_dim_network_from_data(db)
        DimensionService.populate_dim_operator_from_data(db)
        DimensionService.populate_dim_device_from_data(db)

        logger.info("=" * 70)
        logger.info("✓ ALL DIMENSIONS POPULATED")
        logger.info("=" * 70)