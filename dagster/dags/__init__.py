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

    dbt_project_dir = "/dbt"
    profiles_dir = "/dbt"
    
    print(f"Checking if {dbt_project_dir} exists...")
    if not os.path.exists(dbt_project_dir):
        print(f"DBT project dir not found at {dbt_project_dir}. Skipping DBT asset load.")
        return Definitions(jobs=all_jobs)
    
    print(f"✓ DBT project dir exists")
    
    manifest_path = os.path.join(dbt_project_dir, "target", "manifest.json")
    print(f"Checking if manifest exists at: {manifest_path}")
    
    if not os.path.exists(manifest_path):
        print(f"Manifest not found at {manifest_path}. Running dbt parse...")
        try:
            result = subprocess.run(
                ["dbt", "parse", "--profiles-dir", profiles_dir],
                cwd=dbt_project_dir,
                check=True,
                capture_output=True,
                text=True
            )
            print("dbt parse completed successfully")
        except subprocess.CalledProcessError as e:
            print(f"dbt parse failed with return code {e.returncode}")
            print(f"stdout: {e.stdout}")
            return Definitions(jobs=all_jobs)
        except FileNotFoundError:
            print("dbt command not found. Is dbt-core installed?")
            return Definitions(jobs=all_jobs)
    else:
        print(f"✓ Manifest exists")
    
    print("Creating DbtCliResource...")
    dbt = DbtCliResource(
        project_dir=dbt_project_dir,
        profiles_dir=profiles_dir,
    )
    print("✓ DbtCliResource created")
    
    print("Creating DbtProject...")
    dbt_project = DbtProject(
        project_dir=dbt_project_dir,
    )
    print("✓ DbtProject created")
    
    print("Running prepare_if_dev...")
    dbt_project.prepare_if_dev()
    print("✓ prepare_if_dev completed")
    
    print("Creating @dbt_assets...")
    @dbt_assets(manifest=manifest_path)
    def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()
    
    print(f"✓ Created dbt assets: {type(my_dbt_assets)}")
    print(f"✓ Asset keys count: {len(list(my_dbt_assets.keys))}")
    
    print("Creating Definitions...")
    defs_obj = Definitions(
        assets=[my_dbt_assets],
        jobs=all_jobs,
        resources={"dbt": dbt}
    )
    print(f"✓ Definitions created")
    print("=" * 50)
    
    return defs_obj

print("About to call make_defs()...")
defs = make_defs()
print(f"✓ defs assigned: {defs}")
