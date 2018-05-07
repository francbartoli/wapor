import click
import os
from utils.helpers import Name, TIME_RESOLUTION as tr


class Level(click.ParamType):
    name = 'level'

    def level(args):
        pass


class ApiKey(click.ParamType):
    name = 'api-key'

    def apikey(args):
        pass


@click.group()
@click.option(
    '--api-key', '-a',
    type=ApiKey(),
    help='your API key for Earth Engine API',
)
@click.option(
    '--credential-file', '-c',
    type=click.Path(),
    default='~/.serviceaccount.cfg',
)
@click.option(
    '--config-file', '-c',
    type=click.Path(),
    default='config.yaml',
)
@click.option(
    '--level', '-l',
    type=Level(),
    help='Level of the data - Can be L1, L2, L3',
)
@click.pass_context
def main(ctx, api_key, credential_file, config_file, level):
    """
    """
    fn_credential = os.path.expanduser(credential_file)
    if not api_key and os.path.exists(fn_credential):
        with open(fn_credential) as cfg:
            api_key = cfg.read()

    fn_config = os.path.expanduser(config_file)
    # TODO

    # TODO from config file
    ee_workspace_base = "projects"
    ee_workspace_wapor = "fao_wapor"

    ctx.obj = {
        'api_key': api_key,
        'credential_file': fn_credential,
        'EE_WORKSPACE_BASE': ee_workspace_base,
        'EE_WORKSPACE_WAPOR': os.path.join(
            ee_workspace_base,
            ee_workspace_wapor
        ),
        'level': level
    }


@main.command()
@click.argument('year')
@click.argument('season')
@click.argument('temporal_resolution')
@click.argument('input_component')
@click.pass_context
def aet(ctx, year, season, temporal_resolution, input_component):
    """
        wapor -l L1 aet 2016 1 A AET
    """

    from algorithms.aet import AET 

    kwargs = {
        "year": year,
        "season": season,
        "temporal_resolution": temporal_resolution,
        "component": input_component #  AET
    }
    context = ctx.obj.copy()
    context.update(kwargs)

    # Use class Name to express wapor name convention over GEE
    src_image_coll = Name(**context).src_collection()
    print src_image_coll  # L1_AET
    dst_image_coll = Name(**context).dst_collection()
    print dst_image_coll  # L1_AET_A
    dst_asset_coll = Name(**context).dst_assetcollection_id()
    print dst_asset_coll  # projects/fao_wapor/L1_AET_A
    dst_asset_image = Name(**context).dst_image()
    print dst_asset_image  # AET_16
    dst_asset_id = Name(**context).dst_asset_id()
    print dst_asset_id  # projects/fao_wapor/L1_AET_A/AET_16
    if "AET" in src_image_coll:
        if context["temporal_resolution"] in [
            tr.annual.value,
            tr.short_annual.value
        ]:
            i = os.path.join(
                context["EE_WORKSPACE_WAPOR"],
                src_image_coll.replace("AET", "I")
            ) + "_" + tr.short_dekadal.value
            e = os.path.join(
                context["EE_WORKSPACE_WAPOR"], 
                src_image_coll.replace("AET", "E")
            ) + "_" + tr.short_dekadal.value
            t = os.path.join(
                context["EE_WORKSPACE_WAPOR"], 
                src_image_coll.replace("AET", "T")
            ) + "_" + tr.short_dekadal.value
        else:
            raise ValueError("Not implementes yet.")
    else:
        raise ValueError("Wrong value for algorithm not being AET")
    colls = {"collI": i, "collE": e, "collT": t}
    # Create Marmee object instance with specific inputs for AET and filter
    aet = AET(**colls)
    print aet.inputs


if __name__ == "__main__":
    main()
