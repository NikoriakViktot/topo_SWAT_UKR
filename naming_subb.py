import shapely
from shapely import MultiPoint, LineString, GeometryCollection
from shapely.geometry import Point, MultiLineString
from abc import ABC, abstractmethod
import geopandas as gpd
import fiona
from shapely.ops import unary_union
import os
import pandas as pd
import datetime


RIVERS_PATH = "C:\\Users\\user\\Documents\\SWAT_subbasyn\\river\\rivers_UA_RU_MD_BY_projected.shp"
RIVERS_NEW_PATH = "C:\\Users\\user\\Documents\\SWAT_subbasyn\\SWB__Rivers_UKRAINE_23_12_2019\\SWB_R_total.shp"
SUBBASINS_PATH = "C:\\Users\\user\\PycharmProjects\\topo_SWAT_UKR\\Small_subs_100km2_cut.geojson"
RIV1_PATH = "C:\\Users\\user\\Documents\\SWAT_subbasyn\\riv1\\riv1.shp"
FILE_INPUT =  f"C:\\Users\\user\\Documents\\SWAT_subbasyn\\subbasins_update_name_{datetime.datetime.now().strftime('%H_%M_%S')}.geojson"


class GeoDataBuilder(ABC):

    @abstractmethod
    def check_and_change_crs(self):
        pass

    @abstractmethod
    def geometry_buffer_subbasins(self):
        pass

    @abstractmethod
    def geometry_buffer_riv1(self):
        pass

    @abstractmethod
    def initialize_and_set_column_types(self):
        pass

    # @abstractmethod
    # def determine_main_river(self):
    #     pass

    @abstractmethod
    def fragment_subbasins_by_unique_id(self):
        pass

    @abstractmethod
    def compare_and_update_river_names(self):
        pass

    @abstractmethod
    def restore_original_geometry(self):
        pass

    @abstractmethod
    def remove_main_river_column(self):
        pass

    @abstractmethod
    def remove_and_merge_intersections(self):
        pass


    @abstractmethod
    def save_subbasains_new(self):
        pass

    @abstractmethod
    def build_river_hierarchy(self):
        pass

    @abstractmethod
    def add_hierarchy_columns(self):
        pass

    @abstractmethod
    def analyze_river_intersections(self):
        pass

    @abstractmethod
    def update_subbasins_with_main_river(self):
        pass

    @abstractmethod
    def optimize_geometry(self):
        pass


class SubbasinBuilder(GeoDataBuilder):

    def __init__(self):
        self.subbasins_df = gpd.read_file(SUBBASINS_PATH)
        self.rivers = gpd.read_file(RIVERS_PATH)
        self.rivers_new = gpd.read_file(RIVERS_NEW_PATH)
        self.riv1 = gpd.read_file(RIV1_PATH)
        self.subbasins = self.subbasins_df.copy()
        self.original_geometry = self.subbasins['geometry'].copy()
        self.rivers_sindex = self.rivers.sindex
        self.rivers_new_sindex = self.rivers_new.sindex
        self.riv1_sindex = self.riv1.sindex
        self.subbasins_sindex = self.subbasins.sindex
        self.main_river_geometries = {}
        self.intersecting_subbasins_dict = {}
        self.hierarchy = self.build_river_hierarchy()


    def check_and_change_crs(self):
        target_crs = self.rivers.crs
        if self.subbasins.crs != target_crs:
            self.subbasins = self.subbasins.to_crs(target_crs)
        if self.riv1.crs != target_crs:
            self.riv1 = self.riv1.to_crs(target_crs)

    def geometry_buffer_subbasins(self, buffer_size=0):
        self.subbasins['geometry'] = self.subbasins['geometry'].buffer(buffer_size)

    def geometry_buffer_riv1(self, buffer_size=0):
        self.riv1['geometry'] = self.riv1['geometry'].buffer(buffer_size)

    def build_river_hierarchy(self):
        river_hierarchy = {}
        for _, row in self.rivers_new.iterrows():
            river_name = row['NAME_UKR']
            flows_into = row['FLOW_TO']

            if river_name and flows_into:
                key = (river_name, flows_into)
                if key not in river_hierarchy:
                    river_hierarchy[key] = self.get_river_hierarchy(flows_into)

        return river_hierarchy

    def get_river_hierarchy(self, river_name):
        hierarchy = []
        seas = ["Чорне море", "Азовське море"]

        while True:
            # Нормалізація апострофів у назві річки
            normalized_river_name = river_name.replace("'", "’")

            # Екранування апострофів для пошуку в DataFrame
            escaped_river_name = normalized_river_name.replace("'", "\\'")

            matching_rivers = self.rivers_new[self.rivers_new['NAME_UKR'] == escaped_river_name]
            if matching_rivers.empty:
                break
            flow_to = matching_rivers['FLOW_TO'].values[0]
            if not flow_to or flow_to == river_name or flow_to in seas:
                break

            hierarchy.append(flow_to)
            river_name = flow_to

        return hierarchy

    def get_intersecting_geometries(self, geometry, gdf, sindex):
        possible_matches_index = list(sindex.intersection(geometry.bounds))
        possible_matches = gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(geometry)]
        return precise_matches

    def get_river_source_and_mouth(self, river_geometry):
        if isinstance(river_geometry, MultiLineString):
            river_source = Point(river_geometry.geoms[0].coords[0])
            river_mouth = Point(river_geometry.geoms[0].coords[-1])
        else:
            river_source = Point(river_geometry.coords[0])
            river_mouth = Point(river_geometry.coords[-1])
        return river_source, river_mouth

    def get_river_for_subbasin(self, main_river_name, add_prefix=True):
        if main_river_name is None or main_river_name == 'None':
            return None
        canal_indicators = ["канал", "магістральний", "рч-2", "чорноморський", "роздольненська"]
        no_prefix_indicators = ["рук.", "гирло", "лиман", "рукав", "водосховище"]

        if any(indicator in main_river_name.lower() for indicator in canal_indicators):
            return main_river_name
        if any(indicator in main_river_name.lower() for indicator in no_prefix_indicators):
            return main_river_name

        return ("р. " + str(main_river_name)) if add_prefix else main_river_name


    @staticmethod
    def get_distance_to_source(subbasin_geometry, river_source):
        return subbasin_geometry.centroid.distance(river_source)

    def create_subbasin_dictionary(self):
        self.subbasin_dict = {}
        unnamed_counter = 1  # Лічильник для річок "Без назви"

        for idx, subbasin in self.subbasins.iterrows():
            main_river_name = subbasin['MainRiver']

            # Перевірка на відсутність назви річки
            if pd.isna(main_river_name) or main_river_name == 'None':
                continue

            if main_river_name == 'Без назви':
                main_river_name += f"_{unnamed_counter}"
                unnamed_counter += 1

            if main_river_name not in self.subbasin_dict:
                self.subbasin_dict[main_river_name] = []
            self.subbasin_dict[main_river_name].append(idx)

    def fragment_subbasins_by_unique_id(self):
        self.create_subbasin_dictionary()
        for unique_id, subbasin_indices in self.subbasin_dict.items():
            subbasins_group = self.subbasins.iloc[subbasin_indices]
            self.perform_fragmentation_for_group(subbasins_group, unique_id)

    def perform_fragmentation_for_group(self, subbasins_group, unique_id):
        main_river_name = unique_id.split("_")[0]

        # Перевірка на відсутність назви річки
        if pd.isna(main_river_name) or main_river_name == 'null':
            for idx in subbasins_group.index:
                self.subbasins.at[idx, 'Fragment'] = pd.NA
            print(f"Фрагментація пропущена для {unique_id} (немає назви річки)")
            return

        # Продовження звичайної логіки, якщо назва річки є
        river_geometry = self.rivers[self.rivers['name_ua'] == main_river_name].iloc[0]['geometry']
        river_source, _ = self.get_river_source_and_mouth(river_geometry)

        # Обчислення відстані від центроїду підбасейну до джерела річки
        subbasins_group = subbasins_group.copy()
        subbasins_group['DistanceToSource'] = subbasins_group['geometry'].centroid.distance(river_source)
        # Сортування підбасейнів за відстанню до джерела
        sorted_subbasins = subbasins_group.sort_values(by='DistanceToSource')

        print(f"Фрагментація для {unique_id}:")
        if len(sorted_subbasins) == 1:
            subbasin_index = sorted_subbasins.index[0]
            self.subbasins.at[subbasin_index, 'Fragment'] = pd.NA
            print(f"  Один суббасейн {subbasin_index}: Фрагмент 0")
        else:
            for idx, (subbasin_index, _) in enumerate(sorted_subbasins.iterrows(), start=1):
                self.subbasins.at[subbasin_index, 'Fragment'] = int(idx)
                print(f"  Суббасейн {subbasin_index}: Фрагмент {idx}")

        print("Фрагментація завершена\n")

    def analyze_river_intersections(self):
        # Перевірка, чи вже існує збережений файл
        if os.path.exists('max_intersections_dict.csv'):
            # Завантаження даних з файлу
            self.load_max_intersections_dict_from_csv()
        else:
            # Виконання аналізу перетинів річок
            max_intersections_dict = {}
            for river1_idx, river1_row in self.riv1.iterrows():
                subbasin_id = river1_row['Subbasin']
                river_intersections = {}
                for river2_idx, river2_row in self.rivers.iterrows():
                    if river1_row.geometry.intersects(river2_row.geometry):
                        intersection = river1_row.geometry.intersection(river2_row.geometry)
                        intersection_count = self.count_intersections(intersection)
                        river_name = river2_row['name_ua']
                        if river_name in river_intersections:
                            river_intersections[river_name] += intersection_count
                        else:
                            river_intersections[river_name] = intersection_count

                if river_intersections:
                    main_river = max(river_intersections, key=river_intersections.get)
                    max_intersections_dict[subbasin_id] = main_river

            self.max_intersections_dict = max_intersections_dict
            # Збереження результатів у файл
            self.save_max_intersections_dict_to_csv()

    def save_max_intersections_dict_to_csv(self):
        df = pd.DataFrame(list(self.max_intersections_dict.items()), columns=['Subbasin', 'MainRiver'])
        df.to_csv('max_intersections_dict.csv', index=False)

    def load_max_intersections_dict_from_csv(self):
        df = pd.read_csv('max_intersections_dict.csv')
        self.max_intersections_dict = dict(zip(df['Subbasin'], df['MainRiver']))

    def determine_main_river(self, subbasin_id):
        main_river_name = self.max_intersections_dict.get(subbasin_id)
        if main_river_name:
            print(f"Main river for Subbasin {subbasin_id}: {main_river_name}")
            return main_river_name
        else:
            print(f"No intersecting rivers found for Subbasin {subbasin_id}")
            return None

    def update_subbasins_with_main_river(self):
        for idx, row in self.subbasins.iterrows():
            subbasin_id = row['Subbasin']
            main_river_name = self.determine_main_river(subbasin_id)
            if main_river_name:
                self.subbasins.at[idx, 'MainRiver'] = main_river_name
            else:
                self.subbasins.at[idx, 'MainRiver'] = pd.NA

    def count_intersections(self, intersection):
        if intersection.is_empty:
            return 0

        if isinstance(intersection, (Point, LineString)):
            return 1
        elif isinstance(intersection, MultiLineString):
            return sum(1 for _ in intersection.geoms)
        elif isinstance(intersection, GeometryCollection):
            return sum(self.count_intersections(geom) for geom in intersection)
        else:
            return 0

    # def compare_and_update_river_names(self):
    #     for idx, row in self.subbasins.iterrows():
    #         intersecting_rivers_new = self.get_intersecting_geometries(row['geometry'], self.rivers_new,
    #                                                                    self.rivers_new_sindex)
    #
    #         if not intersecting_rivers_new.empty:
    #             main_river_new = intersecting_rivers_new.loc[
    #                 intersecting_rivers_new.geometry.intersection(row['geometry']).length.idxmax()]
    #             main_river_name = main_river_new['NAME_UKR']
    #             flow_to = main_river_new['FLOW_TO']
    #             current_main_river_geometry = self.main_river_geometries.get(idx)
    #
    #             if current_main_river_geometry and current_main_river_geometry.intersects(main_river_new.geometry):
    #                 self.subbasins.at[idx, 'MainRiver'] = main_river_name
    #                 correct_river_name = self.get_river_for_subbasin(main_river_name, add_prefix=True)
    #                 self.subbasins.at[idx, 'Name_UA'] = correct_river_name
    #                 self.subbasins.at[idx, 'FlowTo'] = flow_to
    #             else:
    #                 self.subbasins.at[idx, 'MainRiver'] = main_river_name
    #                 correct_river_name = self.get_river_for_subbasin(main_river_name, add_prefix=True)
    #                 self.subbasins.at[idx, 'Name_UA'] = correct_river_name
    #                 self.subbasins.at[idx, 'FlowTo'] = flow_to
    #         else:
    #             self.subbasins.at[idx, 'MainRiver'] = pd.NA
    #             self.subbasins.at[idx, 'Name_UA'] = pd.NA
    #             self.subbasins.at[idx, 'FlowTo'] = pd.NA

    def compare_and_update_river_names(self):
        for idx, row in self.subbasins.iterrows():
            main_river_name = row['MainRiver']

            if pd.notna(main_river_name):
                correct_river_name = self.get_river_for_subbasin(main_river_name, add_prefix=True)
                self.subbasins.at[idx, 'Name_UA'] = correct_river_name
            else:
                self.subbasins.at[idx, 'Name_UA'] = pd.NA

    def initialize_and_set_column_types(self):
        column_types = {
            'Name_UA': str,
            'Fragment': int,
            'MainRiver': str,
            'FlowTo': str
                    }
        columns_to_initialize = ['Name_UA', 'MainRiver', 'FlowTo', 'Fragment']

        for column in columns_to_initialize:
            if column not in self.subbasins.columns:
                self.subbasins[column] = None
        # Заміна None значень для числових колонок на 0
        for column, column_type in column_types.items():
            if column_type == int:
                self.subbasins[column].fillna(0, inplace=True)
                self.subbasins[column] = self.subbasins[column].astype(int)
            else:
                self.subbasins[column] = self.subbasins[column].astype(column_type)

    def add_hierarchy_columns(self):

        max_hierarchy_length = max(len(hierarchy) for hierarchy in self.hierarchy.values())

        for i in range(2, max_hierarchy_length + 1):
            column_name = f'FlowTo{i}'
            if column_name not in self.subbasins.columns:
                self.subbasins[column_name] = pd.NA
                self.subbasins[column_name] = self.subbasins[column_name].astype('object')

        # Оновлення даних у DataFrame неар
        for idx, row in self.subbasins.iterrows():
            main_river_name = row['MainRiver']
            flow_to = row['FlowTo']
            key = (main_river_name, flow_to)

            if key in self.hierarchy:
                print(self.hierarchy)
                for i, river in enumerate(self.hierarchy[key], start=2):
                    print(i, river)
                    self.subbasins.at[idx, f'FlowTo{i}'] = river


    def remove_main_river_column(self):
        if 'MainRiver' in self.subbasins.columns:
            self.subbasins.drop(columns=['MainRiver'], inplace=True)


    def restore_original_geometry(self):
        self.subbasins['geometry'] = self.original_geometry


    def optimize_geometry(self, buffer_size=0, simplify_tolerance=0):
        self.geometry_buffer_subbasins(buffer_size=buffer_size)
        # Зменшення геометрії
            # Спрощення геометрії
        self.subbasins['geometry'] = self.subbasins['geometry'].simplify(tolerance=simplify_tolerance)

        # # Збільшення геометрії
        # self.geometry_buffer_subbasins(buffer_size=-buffer_size)

    def remove_and_merge_intersections(self):
        for idx, subbasin in self.subbasins.iterrows():
            # 1. Перевірка та виправлення геометрії
            try:
                if not subbasin['geometry'].is_valid:
                    simplified_geometry = subbasin['geometry'].simplify(tolerance=0.01)
                    if simplified_geometry.is_valid:
                        subbasin['geometry'] = simplified_geometry
                    else:
                        subbasin['geometry'] = subbasin['geometry'].buffer(-0.01)
            except Exception as e:
                print(f"Error during geometry simplification or buffering: {e}")
                continue

            # 2. Знаходження можливих перетинів
            try:
                possible_matches_index = list(self.subbasins_sindex.intersection(subbasin['geometry'].bounds))
            except shapely.errors.TopologyException:
                print("Error with geometry during intersection:", subbasin['geometry'])
                continue

            # Перевірка на наявність idx в possible_matches_index
            if idx in possible_matches_index:
                possible_matches_index.remove(idx)

            possible_matches = self.subbasins.iloc[possible_matches_index]
            try:
                precise_matches = possible_matches[possible_matches.intersects(subbasin['geometry'])]
            except Exception as e:
                print(f"Error during precise intersection check: {e}")
                continue

            if not precise_matches.empty:
                # Створення списку геометрій для об'єднання
                geometries_to_union = [subbasin['geometry']]
                for _, intersecting_subbasin in precise_matches.iterrows():
                    try:
                        new_geometry = subbasin['geometry'].difference(intersecting_subbasin['geometry'])
                        geometries_to_union.append(new_geometry)
                    except Exception as e:
                        print(f"Error during geometry difference: {e}")
                        continue

                # Об'єднання геометрій
                try:
                    merged_geometry = unary_union(geometries_to_union)
                    self.subbasins.at[idx, 'geometry'] = merged_geometry  # Змінюємо геометрію в копії DataFrame
                except Exception as e:
                    print(f"Error during geometry union: {e}")
                    continue

                # Оновлення R-tree після зміни геометрії
                self.subbasins_sindex = self.subbasins.sindex
    def save_subbasains_new(self):
        original_crs = "EPSG:4326"
        self.subbasins = self.subbasins.to_crs(original_crs)
        fiona.drvsupport.supported_drivers['GeoJSON'] = 'rw'
        self.subbasins.to_file(FILE_INPUT, driver='GeoJSON')



class GeoDataManager:
    def __init__(self, builder: SubbasinBuilder):
        self.builder = builder

    def construct(self):
        # self.builder.build_river_hierarchy()
        self.builder.check_and_change_crs()
        self.builder.initialize_and_set_column_types()
        # self.builder.geometry_buffer_subbasins(buffer_size=-1)
        self.builder.geometry_buffer_riv1(buffer_size=25)
        self.builder.analyze_river_intersections()
        self.builder.update_subbasins_with_main_river()
        self.builder.optimize_geometry(buffer_size=-300, simplify_tolerance=20)
        self.builder.fragment_subbasins_by_unique_id()
        self.builder.compare_and_update_river_names()
        # self.builder.add_hierarchy_columns()
        self.builder.remove_main_river_column()
        self.builder.restore_original_geometry()
        # self.builder.remove_and_merge_intersections()
        self.builder.save_subbasains_new()


# Client code
builder = SubbasinBuilder()
manager = GeoDataManager(builder)
manager.construct()
