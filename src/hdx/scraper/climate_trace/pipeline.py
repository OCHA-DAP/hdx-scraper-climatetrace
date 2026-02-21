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
            city_url = self._configuration["city_url"].format(admin_id=iso3)
            try:
                city_json = self._retriever.download_json(city_url)
            except DownloadError:
                logger.warning(f"No cities found for {iso3}")
                city_json = []
            self.admins[iso3] = {"admin": admin_info, "city": city_json}
        return

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

    def get_emissions_admin_data(self) -> None:
        min_year = self.min_date.year
        max_year = self.today.year

        # loop through countries, admin units, gases, and years
        base_url = self._configuration["emissions_url"]
        for iso3, admin_types in self.admins.items():
            self.data[iso3] = {}
            for gas, _ in self._configuration["gases"].items():
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
                            rows = self.process_emissions_admin_rows(json, admin_unit)
                            self.data[iso3][f"{gas}|{admin_type}"].extend(rows)
        return

    def get_emissions_source_data(self) -> None:
        min_year = self.min_date.year
        max_year = self.today.year

        # loop through countries, gases, sectors, and years
        base_url = self._configuration["source_url"]
        for iso3, _ in self.admins.items():
            if iso3 not in self.data:
                self.data[iso3] = {}
            for gas, _ in self._configuration["gases"].items():
                self.data[iso3][f"{gas}|source"] = []
                for sector in self._configuration["sectors"]:
                    for year in range(min_year, max_year + 1):
                        for page in range(10000):
                            url = f"{base_url}?sectors={sector}&year={year}&gas={gas}&gadmId={iso3}&limit=10000&offset={page * 10000}"
                            json = self._retriever.download_json(url)
                            if json is None:
                                break
                            rows = self.process_emissions_source_rows(json)
                            self.data[iso3][f"{gas}|source"].extend(rows)
                            if len(json) < 10000:
                                break
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
                if "month" in row:
                    date = f"{row['year']}-{str(row['month']).zfill(2)}"
                    dates.add(date)
                else:
                    dates.add(f"{row['year']}-1")
                    dates.add(f"{row['year']}-12")
                if admin_type == "admin":
                    admin_levels.add(str(row["level"]))
                if not subnational:
                    if admin_type in ["city", "source"] or (
                        admin_type == "admin" and row["level"] > 0
                    ):
                        subnational = True
            admin_name = admin_type
            admin_desc = admin_type
            if admin_type == "admin":
                admin_name = f"admin_{'_'.join(sorted(list(admin_levels)))}"
                admin_desc = f"admin {' and '.join(sorted(list(admin_levels)))}"
            gas_desc = self._configuration["gases"][gas]
            if gas == "co2e_20yr":
                gas = "co2e"
            resource_name = f"{iso3.lower()}_{gas}_{admin_name}.csv"
            resource_info = {
                "name": resource_name,
                "description": f"{country_name} {gas_desc} emissions over the past 2 years at the {admin_desc} level.",
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
        if end_date > self.today.replace(tzinfo=None):
            end_date = self.today.replace(tzinfo=None)
        dataset.set_time_period(start_date, end_date)
        dataset.add_tags(self._configuration["tags"])

        dataset.set_subnational(subnational)
        dataset.add_country_location(iso3)

        return dataset
