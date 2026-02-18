#!/usr/bin/python
"""Climate_trace scraper"""

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
        self.admins = {}
        self.data = {}

    def get_admin_data(self, iso3s: list[str]) -> None:
        for iso3 in iso3s:
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
            cities_url = self._configuration["cities_url"].format(admin_id=iso3)
            try:
                cities_json = self._retriever.download_json(cities_url)
            except DownloadError:
                logger.warning(f"No cities found for {iso3}")
                cities_json = []
            self.admins[iso3] = {"admin": admin_info, "cities": cities_json}
        return

    def process_rows(self, input_data: dict, admin_unit: dict) -> list[dict]:
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

    def get_emissions_admin_data(self) -> None:
        min_year = self.min_date.year
        max_year = self.today.year

        # loop through countries, admin units, gases, and years
        base_url = self._configuration["emissions_url"]
        for iso3, admin_types in self.admins.items():
            self.data[iso3] = {}
            for gas in self._configuration["gases"]:
                for admin_type, admin_units in admin_types.items():
                    self.data[iso3][f"{gas}|{admin_type}"] = []
                    if gas == "pm2_5" and admin_type != "admin":
                        continue
                    for admin_unit in admin_units:
                        admin_id = admin_unit["id"]
                        for year in range(min_year, max_year + 1):
                            if admin_type == "admin":
                                admin_id_type = "gadmId"
                            else:
                                admin_id_type = "cityId"
                            url = f"{base_url}?year={year}&gas={gas}&{admin_id_type}={admin_id}"
                            json = self._retriever.download_json(url)
                            rows = self.process_rows(json, admin_unit)
                            self.data[iso3][f"{gas}|{admin_type}"].extend(rows)
        return

    def generate_country_dataset(self, iso3: str) -> Dataset | None:
        country_name = Country.get_country_name_from_iso3(iso3)
        dataset_name = f"{iso3.lower()}-climate-trace"
        dataset_title = f"{country_name}: {self._configuration['dataset_title']}"

        country_data = self.data.get(iso3)
        if country_data is None:
            return None

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        subnational = False
        dates = set()
        for data_type, rows in country_data.items():
            if len(rows) == 0:
                continue
            gas, admin_type = data_type.split("|")
            admin_levels = set()
            for row in rows:
                date = f"{row['year']}-{row['month']}"
                dates.add(date)
                if admin_type == "admin":
                    admin_levels.add(str(row["level"]))
                if not subnational:
                    if admin_type == "cities" or (
                        admin_type == "admin" and row["level"] > 0
                    ):
                        subnational = True
            admin_name = (
                admin_type
                if admin_type == "cities"
                else f"{admin_type}_{'_'.join(sorted(list(admin_levels)))}"
            )
            resource_name = f"{iso3.lower()}_{gas}_{admin_name}.csv"
            resource_info = {
                "name": resource_name,
                "description": f"Emissions data for {gas} in the past 24 months in {iso3} at the {admin_name.replace('_', ' ')} level",
            }

            dataset.generate_resource(
                self._tempdir,
                resource_info["name"],
                rows,
                resource_info,
            )

        start_date = f"{min(dates)}-1"
        end_year, end_month = max(dates).split("-")
        end_date = datetime(int(end_year), int(end_month), 1)
        end_date = end_date + relativedelta(months=1) - relativedelta(days=1)
        dataset.set_time_period(start_date, end_date)
        dataset.add_tags(self._configuration["tags"])

        dataset.set_subnational(subnational)
        dataset.add_country_location(iso3)

        return dataset
