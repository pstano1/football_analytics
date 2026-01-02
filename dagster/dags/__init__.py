from dagster import Definitions, AssetExecutionContext
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets
import os
import subprocess

from .associations_etl import associations_etl
from .leagues_etl import leagues_etl
from .matches_etl import matches_etl
from .players_etl import players_etl
from .positions_etl import positions_etl
from .referees_etl import referees_etl
from .stadiums_etl import stadiums_etl
from .team_league_season_etl import teams_leagues_seasons_etl
from .teams_etl import teams_etl

def make_defs():
    print("=" * 50)
    print("STARTING make_defs()")
    print("=" * 50)
    
    all_jobs = [
        associations_etl,
        leagues_etl,
        matches_etl,
        players_etl,
        positions_etl,
        referees_etl,
        stadiums_etl,
        teams_leagues_seasons_etl,
        teams_etl
    ]

    projects = {
        "postgres": "/dbt/postgres",
        "clickhouse": "/dbt/clickhouse"
    }

    assets_list = []
    resources_dict = {}

    for name, project_dir in projects.items():
        profiles_dir = project_dir 
        print(f"\n--- Setting up dbt project: {name} at {project_dir} ---")
        
        if not os.path.exists(project_dir):
            print(f"Project dir {project_dir} not found. Skipping...")
            continue

        manifest_path = os.path.join(project_dir, "target", "manifest.json")

        if not os.path.exists(manifest_path):
            print(f"Manifest not found for {name}. Running dbt parse...")
            try:
                subprocess.run(
                    ["dbt", "parse", "--profiles-dir", profiles_dir],
                    cwd=project_dir,
                    check=True,
                    capture_output=True,
                    text=True
                )
                print(f"dbt parse for {name} completed successfully")
            except subprocess.CalledProcessError as e:
                print(f"dbt parse failed for {name}")
                print("stdout:", e.stdout)
                print("stderr:", e.stderr)
                continue
            except FileNotFoundError:
                print("dbt command not found. Is dbt-core installed?")
                continue

        dbt_resource = DbtCliResource(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
        )

        dbt_project = DbtProject(project_dir=project_dir)
        dbt_project.prepare_if_dev()

        @dbt_assets(manifest=manifest_path, name=f"{name}_assets")
        def project_assets(context: AssetExecutionContext, **kwargs):
            dbt = kwargs[f"{name}_dbt"]
            yield from dbt.cli(["build"], context=context).stream()

        assets_list.append(project_assets)
        resources_dict[f"{name}_dbt"] = dbt_resource
        print(f"✓ Created dbt assets for project {name}")

    defs_obj = Definitions(
        assets=assets_list,
        jobs=all_jobs,
        resources=resources_dict
    )

    print(f"✓ Definitions created with {len(assets_list)} dbt asset sets")
    print("=" * 50)

    return defs_obj


print("About to call make_defs()...")
defs = make_defs()
print(f"✓ defs assigned: {defs}")
