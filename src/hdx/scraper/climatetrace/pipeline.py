#!/usr/bin/python
"""climatetrace scraper"""

import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
        today: datetime,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.today = today
        self.min_date = today - relativedelta(years=2)

    def get_admin_data(self, iso3: str) -> tuple[list, list]:
        country_name = Country.get_country_name_from_iso3(iso3)
        admin_info = [
            {
                "full_name": country_name,
                "id": iso3,
                "level": 0,
                "level_0_id": iso3,
                "level_1_id": "",
                "level_2_id": "",
                "name": country_name,
            }
        ]
        admin1_url = self._configuration["admin_url"].format(admin_id=iso3)
        try:
            admin1_json = self._retriever.download_json(admin1_url)
        except DownloadError:
            logger.warning(f"No admin units found for {iso3}")
            admin1_json = []
        admin_info.extend(admin1_json)
        city_url = self._configuration["city_url"].format(admin_id=iso3)
        try:
            city_json = self._retriever.download_json(city_url)
        except DownloadError:
            logger.warning(f"No cities found for {iso3}")
            city_json = []
        return admin_info, city_json

    def process_emissions_admin_rows(
        self, input_data: dict, admin_unit: dict
    ) -> list[dict]:
        rows = []
        min_year = self.min_date.year
        min_month = self.min_date.month
        sector_data = input_data["sectors"]["timeseries"]
        if sector_data is None:
            sector_data = []
        subsector_data = input_data["subsectors"]["timeseries"]
        if subsector_data is None:
            subsector_data = []
        for row in sector_data + subsector_data:
            if row["year"] <= min_year and row["month"] < min_month:
                continue
            new_row = admin_unit | row
            rows.append(new_row)
        return rows

    def process_emissions_source_rows(self, input_data: list[dict]) -> list[dict]:
        rows = []
        min_year = self.min_date.year
        for row in input_data:
            if row["year"] < min_year:
                continue
            rows.append(row)
        return rows

    def get_emissions_admin_data(
        self, admin_info: list, city_json: list
    ) -> tuple[dict, dict]:
        min_year = self.min_date.year
        max_year = self.today.year

        # loop through admin units, gases, and years
        base_url = self._configuration["emissions_url"]
        gases = self._configuration["gases"]

        def get_data_for_type(gas, admin_id_type, admin_units):
            data_for_type = []
            for admin_unit in admin_units:
                admin_id = admin_unit["id"]
                for year in range(min_year, max_year + 1):
                    url = f"{base_url}?year={year}&gas={gas}&{admin_id_type}={admin_id}"
                    json = self._retriever.download_json(url)
                    rows = self.process_emissions_admin_rows(json, admin_unit)
                    data_for_type.extend(rows)
            return data_for_type

        admin_data = {}
        for gas in gases["admin"]:
            admin_data[gas] = get_data_for_type(gas, "gadmId", admin_info)
        city_data = {}
        for gas in gases["city"]:
            city_data[gas] = get_data_for_type(gas, "cityId", city_json)
        return admin_data, city_data

    def get_emissions_source_data(self, iso3) -> dict:
        min_year = self.min_date.year
        max_year = self.today.year

        # loop through gases, sectors, and years
        base_url = self._configuration["source_url"]
        source_data = {}
        for gas in self._configuration["gases"]["source"]:
            source_data[gas] = []
            for sector in self._configuration["sectors"]:
                for year in range(min_year, max_year + 1):
                    for page in range(10000):
                        url = f"{base_url}?sectors={sector}&year={year}&gas={gas}&gadmId={iso3}&limit=10000&offset={page * 10000}"
                        json = self._retriever.download_json(url)
                        if json is None:
                            break
                        rows = self.process_emissions_source_rows(json)
                        source_data[gas].extend(rows)
                        if len(json) < 10000:
                            break
        return source_data

    def generate_country_dataset(
        self, iso3: str, admin_data, city_data, source_data
    ) -> Dataset | None:
        country_name = Country.get_country_name_from_iso3(iso3)
        dataset_name = f"{iso3.lower()}-climate-trace"
        dataset_title = f"{country_name}: {self._configuration['dataset_title']}"

        if not admin_data and not city_data and not source_data:
            return None

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        subnational = False
        dates = set()

        def extract_dates(row):
            if "month" in row:
                dates.add(f"{row['year']}-{str(row['month']).zfill(2)}")
            else:
                dates.add(f"{row['year']}-1")
                dates.add(f"{row['year']}-12")

        def create_resource(gas, rows, suffix, level_desc):
            gas_desc = self._configuration["gas_names"][gas]
            resource_info = {
                "name": f"{iso3.lower()}_{gas}_{suffix}.csv",
                "description": f"{country_name} {gas_desc} emissions over the past 2 years at the {level_desc} level.",
            }
            dataset.generate_resource(
                self._tempdir, resource_info["name"], rows, resource_info
            )

        # --- 1. Process Admin Data ---
        for gas, rows in admin_data.items():
            if not rows:
                continue

            admin_levels = set()
            for row in rows:
                extract_dates(row)
                admin_levels.add(str(row["level"]))
                if row["level"] > 0:
                    subnational = True

            sorted_levels = sorted(admin_levels)
            create_resource(
                gas,
                rows,
                suffix=f"admin_{'_'.join(sorted_levels)}",
                level_desc=f"admin {' and '.join(sorted_levels)}",
            )

        # --- 2. Process City and Source Data ---
        for data_dict, level_name in [(city_data, "city"), (source_data, "source")]:
            for gas, rows in data_dict.items():
                if not rows:
                    continue

                for row in rows:
                    extract_dates(row)

                create_resource(gas, rows, suffix=level_name, level_desc=level_name)
                subnational = True

        start_date = f"{min(dates)}-1"
        end_year, end_month = max(dates).split("-")
        end_date = datetime(int(end_year), int(end_month), 1)
        end_date = end_date + relativedelta(months=1) - relativedelta(days=1)
        if end_date > self.today.replace(tzinfo=None):
            end_date = self.today.replace(tzinfo=None)
        dataset.set_time_period(start_date, end_date)
        dataset.add_tags(self._configuration["tags"])

        dataset.set_subnational(subnational)
        dataset.add_country_location(iso3)

        return dataset
