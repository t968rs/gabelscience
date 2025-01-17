import math
from shapely.geometry import Polygon, Point
import geopandas as gpd
import os

from src.d00_utils.open_spatial import open_fc_any
from src.d00_utils.time_reporting import StopWatch
from src.d00_utils.unique_id import create_all_unique_id


class VisvalingamSimplifier:
    """
    Implements a basic Visvalingam-Whyatt (VW) polygon simplification,
    optionally with the Weighted (sqrt area) approach.

    NOTE: This handles only the polygon's exterior ring.
    It removes points until all 'triangle areas' exceed 'threshold'.
    """

    def __init__(self, polygon: Polygon):
        if not polygon.is_valid:
            raise ValueError("Input polygon is invalid (self-intersecting or empty).")
        if polygon.is_empty:
            raise ValueError("Input polygon is empty.")
        if polygon.geom_type != "Polygon":
            raise ValueError(f"Expected Polygon, got {polygon.geom_type}.")

        # We'll only process the exterior ring in this demo
        self.orig_polygon = polygon
        self.coords = list(polygon.exterior.coords)  # [(x0, y0), (x1, y1), ...]
        if len(self.coords) < 4:
            raise ValueError("Polygon exterior ring must have at least 4 points (including closing).")

    def simplify(self, threshold: float, weighted: bool = True) -> Polygon:
        """
        Perform Visvalingam-Whyatt simplification on the exterior ring.

        :param threshold: The minimum area (or sqrt(area) if weighted=True) below which points are removed.
        :param weighted: If True, uses Weighted VW (sqrt of area).
        :return: A new simplified Polygon.
        """

        # We'll do a naive iterative approach:
        # 1) Calculate "triangle area" for each interior point i
        # 2) Find the point with smallest area
        # 3) If area < threshold, remove the point
        # 4) Recompute neighbor areas
        # 5) Stop when smallest area >= threshold or no more removable points.

        # The first & last coords are the same in a closed ring, so we skip them from removal
        # (the ring must stay closed). We'll keep them fixed.

        # Edge case: If threshold is very high, we might remove a lot of points.

        if threshold <= 0:
            # If threshold <= 0, we basically keep all points (no removal)
            return self.orig_polygon

        # We'll treat coords as an open ring internally: 0..n-1, with 0 == n-1 in a closed ring
        # In shapely, the last point = first point for a ring. We'll keep that for geometry correctness.
        # We'll store indexes for the "middle" points that are eligible for removal.
        # The first and last indices are off-limits for removal.
        coords = self.coords[:]

        # Function to compute the VW area of point i, given i-1, i, i+1
        def vw_area(vwi: int) -> float:
            """
            Returns the VW area measure for coords[idx], i.e. the area of the
            triangle formed by coords[idx-1], coords[idx], coords[idx+1].
            Weighted => sqrt(area).
            """
            x1, y1 = coords[vwi - 1]
            x2, y2 = coords[vwi]
            x3, y3 = coords[(vwi + 1) % (len(coords))]
            # Triangle area = |x1(y2 - y3) + x2(y3 - y1) + x3(y1 - y2)| / 2
            tri_area = abs(x1 * (y2 - y3) + x2 * (y3 - y1) + x3 * (y1 - y2)) / 2.0
            if weighted:
                return math.sqrt(tri_area)
            else:
                return tri_area

        # Create a list to track the area of each interior point.
        # We'll say indices from 1..(n-2) are interior. (n-1 is same as index 0 in a ring).
        if len(coords) <= 3:
            # If there's no middle, can't remove anything
            return Polygon(coords)

        interior_indices = list(range(1, len(coords) - 1))  # e.g. [1,2,...,n-2]

        # Precompute areas
        areas = {i: vw_area(i) for i in interior_indices}

        while True:
            # Find the point with the smallest area
            i_min = min(areas, key=areas.get) if areas else None
            if i_min is None:
                break  # No interior points to remove
            min_area = areas[i_min]

            if min_area >= threshold:
                # All remaining points have area >= threshold; done
                break

            # Otherwise, remove that point from coords
            coords.pop(i_min)

            # Also remove it from the interior_indices + areas
            interior_indices.remove(i_min)
            del areas[i_min]

            # We need to recalc area for neighbors i_min-1, i_min, i_min+1
            # because removing a point changes the triangle for neighbors
            # but only if they're still in interior_indices
            for neighbor in [i_min - 1, i_min, i_min + 1]:
                if neighbor in interior_indices:
                    areas[neighbor] = vw_area(neighbor)
                # If neighbor is equal to length of coords, mod it
                elif neighbor == len(coords):
                    # wrap around (closing ring)
                    idx_wrap = 0
                    if idx_wrap in interior_indices:
                        areas[idx_wrap] = vw_area(idx_wrap)

            # Also we might need to shift indices above i_min in interior_indices
            # because coords has shrunk. In a naive approach, we can just rebuild:
            new_interior = []
            for idx in interior_indices:
                if idx > i_min:
                    new_interior.append(idx - 1)
                else:
                    new_interior.append(idx)
            interior_indices = sorted(new_interior)
            # Rebuild the areas dictionary for those new indices:
            new_areas = {}
            for idx in interior_indices:
                new_areas[idx] = vw_area(idx)
            areas = new_areas

        # Return a new simplified polygon
        # Ensure the ring is still closed: the first == last
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        simplified_poly = Polygon(coords)
        return simplified_poly


# -----------------------------------------------------------------------
# DEMO USAGE:
if __name__ == "__main__":
    # Example polygon with too many vertices in the exterior ring
    stopwatch = StopWatch(workspace=os.path.dirname(os.path.abspath(__file__)), options={})
    stopwatch.start_new_iteration()

    shp_path = r"Z:\Shell_Rock\02_WORKING\shellrock_mapping\02_Generalize\S_FLD_HAZ_AR_04.shp"
    input_folder, input_filename = os.path.split(shp_path)
    input_basename, input_ext = os.path.splitext(input_filename)

    gdf = open_fc_any(shp_path)
    create_all_unique_id(gdf)

    crs = gdf.crs
    print(f"Input CRS: {crs}"
          f"\nInput Columns: \n{gdf.columns}")

    # Get just the largest polygon
    largest_poly = gdf.loc[gdf.area.idxmax()]
    fid = largest_poly["unique_id"]

    # Get the coordinates
    pg_coords = largest_poly.geometry.exterior.coords[:]
    poly = Polygon(pg_coords)
    input_length = poly.length

    stopwatch.record_time("Created Polygon")

    vsimpl = VisvalingamSimplifier(poly)
    simplified_polygon = vsimpl.simplify(threshold=0.05, weighted=True)
    simplified_length = simplified_polygon.length

    stopwatch.record_time("Simplified Polygon")

    # export to shp
    output_gdf = gpd.GeoDataFrame(geometry=[simplified_polygon], crs=crs)

    outpath = input_folder + os.path.sep + f"simplified_{input_basename}_fid{fid}{input_ext}"
    output_gdf.to_file(outpath,
        driver="ESRI Shapefile")
    stopwatch.record_time("Exported to Shapefile")

    original_points = len(poly.exterior.coords)
    simplified_points = len(simplified_polygon.exterior.coords)

    points_per_input = original_points / input_length
    points_per_simplified = simplified_points / simplified_length

    print(f"Original #Points: {len(poly.exterior.coords)}")
    if input_length:
        print(f"Original Points Per Length: {round(points_per_input, 2)}")
    print(f"Simplified #Points: {len(simplified_polygon.exterior.coords)}")
    if simplified_length:
        print(f"Simplified Points Per Length: {round(points_per_simplified, 2)}")
