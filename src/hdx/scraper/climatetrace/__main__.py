#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.location.country import Country
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.climatetrace._version import __version__
from hdx.scraper.climatetrace.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-climatetrace"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Climate TRACE"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("climate-trace")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            today = now_utc()
            pipeline = Pipeline(configuration, retriever, tempdir, today)
            countries = [
                {"iso3": x} for x in Country.countriesdata()["countries"].keys()
            ]
            for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                iso3 = nextdict["iso3"]
                admin_info, city_json = pipeline.get_admin_data(iso3)
                admin_data, city_data = pipeline.get_emissions_admin_data(
                    admin_info, city_json
                )
                source_data = pipeline.get_emissions_source_data(iso3)

                logger.info(f"Generating dataset for {iso3}")
                dataset = pipeline.generate_country_dataset(
                    iso3, admin_data, city_data, source_data
                )
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        match_resource_order=True,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
