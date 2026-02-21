from datetime import datetime
from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.climate_trace.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "Test_ClimateTRACE",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                today = datetime(2026, 2, 15)
                pipeline = Pipeline(configuration, retriever, tempdir, today)
                pipeline.get_admin_data(["AFG"])
                pipeline.get_emissions_admin_data()
                pipeline.get_emissions_source_data()

                dataset = pipeline.generate_country_dataset("AFG")
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "afg-climate-trace",
                    "title": "Afghanistan: Climate TRACE Greenhouse Gas and Air Pollutant Emissions",
                    "dataset_date": "[2024-02-01T00:00:00 TO 2025-12-31T23:59:59]",
                    "tags": [
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "points of interest-poi",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "subnational": "1",
                    "groups": [{"name": "afg"}],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "Climate TRACE identifies the most significant gaps in emissions reporting within current greenhouse gas inventories and develops models to estimate those emissions gaps. Climate TRACE emissions data is generated on a sector-by-sector basis by the relevant Climate TRACE sector lead and additional contributed organizations they collaborate with.\n\nEach Climate TRACE sector uses a methodology that is specialized and relevant to each particular sector to estimate emissions. The most common method is using computer vision models to combine one predictor variable (such as satellite imagery) and one high-quality source of ground truth data (such as verified, calibrated sensors located at individual facilities). Computer vision models are a form of artificial intelligence that, unlike generative AI, is relatively easy to verify and not prone to hallucination. In addition, Climate TRACE also uses a number of other statistical approaches such as disaggregating total production within a country to all the facilities, and modeling emissions using IPCC National GHG Inventory Guidelines.\n\nWe’ve published detailed documentation on our methodologies, peer reviewed research that underlies those methodologies, and the key datasets and assumptions for the Climate TRACE inventory. You can find those on our data downloads page at https://climatetrace.org/data.\n",
                    "caveats": "See methodology documents for sector specific caveats",
                    "dataset_source": "Climate TRACE",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "8c24c8ce-f313-4020-bda6-00ae42b2aef1",
                    "owner_org": "climate-trace",
                    "data_update_frequency": 30,
                    "notes": "Climate TRACE is a non-profit coalition of organizations building a timely, open, and accessible inventory of exactly where greenhouse gas emissions are coming from. Climate TRACE estimates greenhouse gas (GHG) and air pollutant emissions for over 2.7 million sources (from over 744 million assets), and every single country globally.\n\nThe Climate TRACE emissions inventory includes:\n- Annual country-level emissions by sub-sector and by gas beginning in 2015\n- Monthly source-level emissions by sub-sector and gas beginning in 2021 and confidence\n- Emissions source ownership where and when available.\n",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "afg_co2e_admin_0_1.csv",
                        "description": "Afghanistan carbon dioxide equivalent (CO2e) emissions over the past 2 years at the admin 0 and 1 level.",
                        "format": "csv",
                    },
                    {
                        "name": "afg_co2e_city.csv",
                        "description": "Afghanistan carbon dioxide equivalent (CO2e) emissions over the past 2 years at the city level.",
                        "format": "csv",
                    },
                    {
                        "name": "afg_pm2_5_admin_0.csv",
                        "description": "Afghanistan PM2.5 emissions over the past 2 years at the admin 0 level.",
                        "format": "csv",
                    },
                    {
                        "name": "afg_co2e_source.csv",
                        "description": "Afghanistan carbon dioxide equivalent (CO2e) emissions over the past 2 years at the source level.",
                        "format": "csv",
                    },
                    {
                        "name": "afg_pm2_5_source.csv",
                        "description": "Afghanistan PM2.5 emissions over the past 2 years at the source level.",
                        "format": "csv",
                    },
                ]

                for resource in resources:
                    file_name = resource["name"]
                    assert_files_same(
                        join(fixtures_dir, file_name),
                        join(tempdir, file_name),
                    )
