import math


class PointLatLng:
    """Simple data object for latitude and longitude."""
    latitude: float
    longitude: float


class GeoHelpers:
    """Minimal port of Fraunhofer GeoHelpers for distance & bearing calculations."""

    RADIUS_EARTH_KM = 6371.0

    # --- Conversion helpers ---
    @staticmethod
    def deg2rad(deg: float) -> float:
        return math.radians(deg)

    @staticmethod
    def rad2deg(rad: float) -> float:
        return math.degrees(rad)

    # --- Distance ---
    @staticmethod
    def distance_in_meters(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        """Compute the great-circle distance between two coordinates in meters."""
        R = GeoHelpers.RADIUS_EARTH_KM
        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c * 1000.0  # meters
        # --- Bearing ---

    @staticmethod
    def bearing(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        """Calculate the initial bearing (0..360°) from point 1 to point 2."""
        lat1, lat2 = map(math.radians, [lat1, lat2])
        dlon = math.radians(lon2 - lon1)

        y = math.sin(dlon) * math.cos(lat2)
        x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
        bearing = math.degrees(math.atan2(y, x))
        return (bearing + 360) % 360

    # --- Midpoint (optional) ---
    @staticmethod
    def midpoint(lon1: float, lat1: float, lon2: float, lat2: float) -> PointLatLng:
        """Return midpoint between two lat/lon points."""
        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1

        bx = math.cos(lat2) * math.cos(dlon)
        by = math.cos(lat2) * math.sin(dlon)
        lat3 = math.atan2(
            math.sin(lat1) + math.sin(lat2),
            math.sqrt((math.cos(lat1) + bx) ** 2 + by ** 2),
        )
        lon3 = lon1 + math.atan2(by, math.cos(lat1) + bx)
        lon3 = (lon3 + 3 * math.pi) % (2 * math.pi) - math.pi  # normalize

        return PointLatLng(latitude=math.degrees(lat3), longitude=math.degrees(lon3))