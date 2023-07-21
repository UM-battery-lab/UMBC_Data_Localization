"""Module for Filter Class for Requests"""
from __future__ import annotations
import typing as t
from dataclasses import dataclass, field

COMPARATOR_LIST = ["gt", "gte", "lt", "lte", "eq", "neq", "icontains", "in"]
FILTER_KEYS = ["tag", "name", "test_records", "test_record", "device", "author", "comment", "project"]


@dataclass
class Filter:
    """Dataclass for individual filter"""

    key: str
    value: t.Any
    comparator: str
    filter_type: str = ""

    def __post_init__(self):
        """Post Processing for determining filter_types
         (tag, metadata, notebook_field, name, etc.)
         """
        self.filter_type = self.key if self.key in FILTER_KEYS else "metadata"
        if self.comparator not in COMPARATOR_LIST:
            raise ValueError(f"Invalid Comparison in filter. Please use one of the following: {*COMPARATOR_LIST,}")



@dataclass
class Filterset:
    """Collection of filters for test record and devices."""

    filters: t.List[Filter] = field(default_factory=list)
    
    
    def add_filter(self, key, value, compare: str = "eq"):
        """Add filter to the filter set"""
        self.filters.append(Filter(key, value, compare))


    def parse_filter(self, filter_obj: str = ""):
        """Construct query string based on the filterset

        Returns
            -------
            String
                string of query for API call
        """
        query = []

        if len(self.filters) < 1:
            raise ValueError("No Filter Provided")
        
        for f in self.filters:
            # Tag is list of strings, django query won't work. Filtering is handled by the REST API
            comparator = check_comp(f.comparator) if f.key != "tag" else "="
            if isinstance(f.value, list) and f.comparator == "in":
                    f.value = ",".join(map(str, f.value))
            if f.filter_type == "metadata":  
                q_param = check_metadata(f.key, filter_obj)
                query.append(f"{q_param}={f.key}&{f.key}{comparator}{f.value}")
            else:
                query.append(f"{f.key}{comparator}{f.value}")
        return "&".join(query)


def check_metadata(key, filter_obj):
    """Helper function that checks if the given key is valid"""
    if filter_obj == "device" and key not in DEVICE_METADATA_KEYS:
        return "notebook_field"
    else:
        return "metadata_key"


def check_comp(comparator: str):
    """Helper function that checks comparator"""
    if comparator == "eq":
        return "="
    elif comparator == "neq":
        return "!="
    else:
        return "__" + comparator + "="


DEVICE_METADATA_KEYS = [
    'area',
    'cell_mass',
    'material_mass',
    'nominal_capacity',
    'lot_number',
    'lot_name',
    'batch_manufacture_date',
    'supplier_part',
    'part_supplier_name',
    'supplier_name',
    'device_form_factor',
    'device_type',
    'device_chemistry',
    'device_chemistry_common_name',
    'device_nominal_voltage',
    'device_weight',
    'device_length_without_terminals',
    'device_width_without_terminals',
    'device_thickness_without_terminals',
    'device_diameter_without_terminals',
    'device_volume',
    'initial_ovc',
    'initial_acir',
    'max_cell_charge_voltage',
    'cell_thermal_cut_off',
    'cathode_id',
    'cathode_material',
    'cathode_loading',
    'cathode_build_date',
    'number_of_cathode_layers',
    'anode_id',
    'anode_material',
    'anode_loading',
    'anode_build_date',
    'number_of_anode_layers',
    'electrolyte_id',
    'electrolyte_material',
    'electrolyte_additive_material',
    'electrolyte_additive_weight_percentage',
    'actual_electrolyte_fill_mass',
    'electrolyte_fill_date',
    'separator_id',
    'separator_base_film_material',
    'separator_coating_material',
    'separator_coating_particle_size',
    'separator_binder_type',
    'separator_solvent_type',
    'separator_batch_name',
    'separator_supplier_name',
    'separator_grade',
    'separator_base_film_thickness',
    'number_of_cells_in_series',
    'number_of_cells_in_parallel',
    'pack_configuration',
    'battery_size_factor',
    'max_pack_charge_voltage',
    'gas_gauge_number',
    'gas_gauge_name',
    'pack_on_frame',
    'supplier_name_pack',
    'pack_batch_manufacture_date',
    'module_number'
 ]
